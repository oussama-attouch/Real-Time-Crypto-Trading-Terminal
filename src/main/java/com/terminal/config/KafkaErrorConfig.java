package com.terminal.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Poison-pill containment: retry briefly, then park the record on the DLT so the
 * partition keeps moving.
 *
 * <h2>The prerequisite nobody mentions</h2>
 * <p>This config is inert on its own. A {@code JsonDeserializer} failure happens
 * <em>before</em> a record exists, so a plain {@code DefaultErrorHandler} would never
 * be called: the container would re-poll the same offset forever and the partition
 * would stall — the exact "blocks the queue" outcome we are trying to avoid. The fix
 * lives in {@code application.yml}: the consumer uses
 * {@code ErrorHandlingDeserializer} with {@code JsonDeserializer} as its delegate, which
 * converts a parse failure into a recoverable record (carrying the original bytes and
 * the exception) instead of an endless loop.
 *
 * <h2>Retry policy</h2>
 * <p>{@link FixedBackOff}(1000 ms, 2 retries) = <b>3 total attempts</b> (initial + 2),
 * then the record is published to the DLT and the offset is committed. A deterministic
 * parse failure will obviously fail all three times; the retries exist for transient
 * handler failures, not for the bytes.
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
public class KafkaErrorConfig {

    private static final long RETRY_INTERVAL_MS = 1_000L;
    /** Retries after the initial delivery: 2 + 1 = 3 attempts, per the spec. */
    private static final long RETRIES = 2L;

    /**
     * Publishes failed records to {@code binance-raw-trades.DLT}, preserving the source
     * partition so a DLT record can be traced back to where it came from.
     */
    @Bean
    DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaProperties kafkaProperties,
                                                               KafkaPipelineProperties pipelineProperties) {
        KafkaTemplate<String, Object> dltTemplate = deadLetterKafkaTemplate(kafkaProperties);
        String dltTopic = pipelineProperties.topics().rawTradesDlt();

        return new DeadLetterPublishingRecoverer(dltTemplate, (record, exception) -> {
            // -1 would mean "partition unknown"; pinning it to 0 keeps the
            // TopicPartition constructible and the record still findable.
            int partition = Math.max(record.partition(), 0);
            log.warn("Publishing poison record (topic={}, partition={}, offset={}, key={}) to {}",
                    record.topic(), record.partition(), record.offset(), record.key(), dltTopic);
            return new TopicPartition(dltTopic, partition);
        });
    }

    /**
     * Registered as a {@code CommonErrorHandler} bean; Boot's auto-configured
     * {@code kafkaListenerContainerFactory} picks it up, so {@code @KafkaListener}
     * needs no extra attribute.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        DefaultErrorHandler errorHandler =
                new DefaultErrorHandler(recoverer, new FixedBackOff(RETRY_INTERVAL_MS, RETRIES));
        // Deterministic parse failures can never succeed, but the spec asks for 3
        // attempts; uncomment to send them to the DLT on the first failure instead of
        // burning two seconds per poison record.
        // errorHandler.addNotRetryableExceptions(DeserializationException.class);
        return errorHandler;
    }

    /**
     * A dedicated template for the DLT.
     *
     * <h2>Why this is not a bean</h2>
     * <p>Declaring a {@code ProducerFactory} or {@code KafkaTemplate} bean would be a
     * trap: Boot's auto-configuration guards both with
     * {@code @ConditionalOnMissingBean}, so an extra bean of either type silently
     * replaces the application's producer — and then
     * {@code KafkaTemplate<String, TradeEvent>} would no longer resolve in the ingestion
     * pipeline. The instance is kept local, and Kafka's producer network thread is a
     * daemon, so it cannot hold the JVM open at shutdown.
     *
     * <p>{@code max.block.ms} is deliberately short: the recoverer runs on the Kafka
     * listener thread, so an unavailable DLT must not wedge a consumer for minutes.
     */
    private KafkaTemplate<String, Object> deadLetterKafkaTemplate(KafkaProperties kafkaProperties) {
        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        producerConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        producerConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        producerConfig.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        producerConfig.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        producerConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);

        DefaultKafkaProducerFactory<String, Object> producerFactory = new DefaultKafkaProducerFactory<>(
                producerConfig, new StringSerializer(), new DeadLetterSerializer());
        return new KafkaTemplate<>(producerFactory);
    }

    /**
     * Writes the failed value to the DLT in whatever form it arrived.
     *
     * <p>This matters because the two failure classes hand the recoverer different
     * types: a deserialization failure yields the <b>raw bytes</b> (from
     * {@code ErrorHandlingDeserializer}), while a handler failure yields an already
     * deserialized {@code TradeEvent}. A {@code JsonSerializer} alone would turn the
     * raw bytes into a base64 JSON string, which is a DLT record nobody can read or
     * replay; a {@code ByteArraySerializer} alone would throw on the {@code TradeEvent}.
     * So: bytes pass through verbatim, everything else is serialized as JSON.
     */
    static final class DeadLetterSerializer implements Serializer<Object> {

        private final JsonSerializer<Object> json = new JsonSerializer<>();

        @Override
        public byte[] serialize(String topic, Object data) {
            if (data == null) {
                return null;
            }
            if (data instanceof byte[] raw) {
                return raw;
            }
            if (data instanceof String text) {
                return text.getBytes(StandardCharsets.UTF_8);
            }
            return json.serialize(topic, data);
        }
    }
}
