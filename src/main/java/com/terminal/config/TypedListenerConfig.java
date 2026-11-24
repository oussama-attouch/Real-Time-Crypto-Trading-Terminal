package com.terminal.config;

import com.terminal.aggregation.CandleEvent;
import com.terminal.ingestion.DepthEvent;
import com.terminal.orders.OrderPlacedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

/**
 * Container factories for the topics that do <b>not</b> carry {@code TradeEvent}.
 *
 * <h2>Why these cannot use the auto-configured factory</h2>
 * <p>{@code application.yml} pins
 * {@code spring.kafka.consumer.properties.spring.json.value.default.type} to
 * {@code com.terminal.ingestion.TradeEvent}, because the trade topic must deserialize into
 * that record without type headers on the wire. Boot builds exactly one
 * {@code ConsumerFactory} from those properties, and it configures its
 * {@code JsonDeserializer} once with that target type baked in. A per-listener
 * {@code @KafkaListener(properties = ...)} override cannot change that: it alters the config
 * map handed to the consumer, not the already-configured deserializer instance.
 *
 * <p>So a depth frame or an order event would bind into {@code TradeEvent} and — because
 * that record is {@code @JsonIgnoreProperties(ignoreUnknown = true)} — silently produce a
 * TradeEvent full of nulls instead of failing. Explicit, per-type factories are the only
 * honest fix.
 *
 * <h2>Two traps encoded here</h2>
 * <ol>
 *   <li><b>No {@code ConsumerFactory} beans.</b> Boot guards its own with
 *       {@code @ConditionalOnMissingBean(ConsumerFactory.class)}, so contributing one
 *       silently replaces the application's and every listener without an explicit factory
 *       ends up on the wrong deserializer. The factories are therefore built <em>inside</em>
 *       these bean methods.</li>
 *   <li><b>Configure through properties only.</b> A {@code JsonDeserializer} refuses to be
 *       configured by setters and configuration properties at the same time
 *       ({@code "must be configured with property setters, or via configuration
 *       properties; not both"}). Everything here goes into the map; no instances are
 *       passed in.</li>
 * </ol>
 */
@Configuration(proxyBeanMethods = false)
public class TypedListenerConfig {

    /** Belongs to ErrorHandlingDeserializer and is meaningless once it is not used. */
    private static final String ERROR_HANDLING_DELEGATE_KEY = "spring.deserializer.value.delegate.class";

    /** Depth frames carry no type headers; the target type is fixed per topic. */
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, DepthEvent> depthListenerContainerFactory(
            KafkaProperties kafkaProperties) {
        return containerFactory(kafkaProperties, DepthEvent.class);
    }

    /** Order events, likewise fixed-type and header-free. */
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, OrderPlacedEvent> orderListenerContainerFactory(
            KafkaProperties kafkaProperties) {
        return containerFactory(kafkaProperties, OrderPlacedEvent.class);
    }

    /**
     * Finalized 1m candles. Without this, {@code CandleKafkaConsumer} binds into
     * {@code TradeEvent} (the global default type), fails, and every candle lands in the
     * dead-letter topic — which is why {@code /topic/klines} was empty and the chart had
     * only the live candle to draw.
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, CandleEvent> candleListenerContainerFactory(
            KafkaProperties kafkaProperties) {
        return containerFactory(kafkaProperties, CandleEvent.class);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> containerFactory(
            KafkaProperties kafkaProperties, Class<T> valueType) {

        // Common settings (bootstrap servers, group id, fetch tuning) still come from
        // application.yml, so nothing drifts from the single source of truth.
        Map<String, Object> config = kafkaProperties.buildConsumerProperties();

        config.remove(ERROR_HANDLING_DELEGATE_KEY);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, valueType.getName());
        config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.terminal");

        ConcurrentKafkaListenerContainerFactory<String, T> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(config));
        // Commit per record: both payloads are superseded almost immediately, so replaying
        // one after a rebalance buys nothing.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        // Default error handling (log, retry, skip) is right for these: neither is worth a
        // dead-letter topic, and neither may block its partition.
        return factory;
    }
}
