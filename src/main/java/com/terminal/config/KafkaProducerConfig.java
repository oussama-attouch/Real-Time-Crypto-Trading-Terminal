package com.terminal.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.Map;

/**
 * Kafka producer-side wiring.
 *
 * <h2>What is intentionally NOT declared here</h2>
 * <p>No {@code ProducerFactory} and no {@code KafkaTemplate} bean. Boot's
 * auto-configuration already builds both from {@code spring.kafka.producer.*},
 * which is where the serializers, {@code acks=all}, idempotence, batching,
 * compression and {@code max.block.ms} live. Declaring them here would duplicate
 * that configuration and immediately rot: a property added to {@code
 * application.yml} would silently stop having any effect. The only thing
 * {@code application.yml} cannot express is topic <em>provisioning</em> — that is
 * what this class is for.
 *
 * <p>{@code KafkaTemplate<String, TradeEvent>} resolves against Boot's
 * {@code KafkaTemplate<?, ?>} bean: Spring's generic matching falls back to the
 * raw type for bean definitions whose type variables are unresolved.
 *
 * <h2>Provisioning</h2>
 * <p>{@code NewTopic} beans are created by the {@code KafkaAdmin} Boot registers.
 * With {@code spring.kafka.admin.fail-fast=false} a missing broker produces a
 * warning and a background retry instead of a failed startup — the service must
 * boot during a Kafka outage.
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
public class KafkaProducerConfig {

    /**
     * {@code binance-raw-trades} — the pipeline's source of truth.
     *
     * <p>Created explicitly rather than relying on broker auto-creation, because
     * auto-created topics inherit broker defaults: the wrong partition count, the
     * wrong retention, and {@code auto.create.topics.enable} false in anything
     * resembling production. Declaring it here means the layout is reviewed, not
     * inherited.
     */
    @Bean
    NewTopic binanceRawTradesTopic(KafkaPipelineProperties properties) {
        KafkaPipelineProperties.Topics topics = properties.topics();
        log.info("Declaring topic '{}' :: partitions={} replicas={} retention={}",
                topics.rawTrades(), topics.partitions(), topics.replicas(), topics.retention());
        return TopicBuilder.name(topics.rawTrades())
                .partitions(topics.partitions())
                .replicas(topics.replicas())
                .configs(Map.of(
                        TopicConfig.RETENTION_MS_CONFIG, Long.toString(topics.retention().toMillis()),
                        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE))
                .build();
    }

    /**
     * {@code binance-klines-1m} — finalized 1-minute candles from the aggregator.
     *
     * <p>Same partition count, replication and key (symbol) as the trade topic, so
     * per-symbol candle ordering holds for exactly the same reason trade ordering does.
     */
    @Bean
    NewTopic binanceKlines1mTopic(KafkaPipelineProperties properties) {
        KafkaPipelineProperties.Topics topics = properties.topics();
        log.info("Declaring topic '{}' :: partitions={} replicas={} retention={}",
                topics.klines1m(), topics.partitions(), topics.replicas(), topics.retention());
        return TopicBuilder.name(topics.klines1m())
                .partitions(topics.partitions())
                .replicas(topics.replicas())
                .configs(Map.of(
                        TopicConfig.RETENTION_MS_CONFIG, Long.toString(topics.retention().toMillis()),
                        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE))
                .build();
    }

    /**
     * {@code binance-depth} — partial L2 snapshots ({@code @depth20@100ms}) as they arrive
     * from Binance.
     *
     * <p>Same key discipline as the trade topic (symbol), so per-symbol depth ordering is
     * guaranteed by the same mechanism. Retention is short by intent: a depth snapshot is
     * worthless the moment the next one arrives — it is here for replay/debugging and for
     * fan-out to consumers that need the book, not as a historical record.
     */
    @Bean
    NewTopic binanceDepthTopic(KafkaPipelineProperties properties) {
        KafkaPipelineProperties.Topics topics = properties.topics();
        log.info("Declaring topic '{}' :: partitions={} replicas={} retention={}",
                topics.depth(), topics.partitions(), topics.replicas(), topics.retention());
        return TopicBuilder.name(topics.depth())
                .partitions(topics.partitions())
                .replicas(topics.replicas())
                .configs(Map.of(
                        TopicConfig.RETENTION_MS_CONFIG, Long.toString(topics.retention().toMillis()),
                        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE))
                .build();
    }

    /**
     * {@code order-events} — orders accepted by the write side (Phase 8).
     *
     * <p>The same key-as-symbol discipline as every other topic. Retention matches the rest
     * because this is an event log for the read side, not a durable audit trail; a real
     * system would keep it longer and pair it with an outbox table.
     */
    @Bean
    NewTopic orderEventsTopic(KafkaPipelineProperties properties) {
        KafkaPipelineProperties.Topics topics = properties.topics();
        log.info("Declaring topic '{}' :: partitions={} replicas={} retention={}",
                topics.orders(), topics.partitions(), topics.replicas(), topics.retention());
        return TopicBuilder.name(topics.orders())
                .partitions(topics.partitions())
                .replicas(topics.replicas())
                .configs(Map.of(
                        TopicConfig.RETENTION_MS_CONFIG, Long.toString(topics.retention().toMillis()),
                        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE))
                .build();
    }

    /**
     * {@code binance-raw-trades.DLT} — poison messages (records the Step 3
     * consumer cannot deserialize or handle). Provisioned now, used later: a
     * dead-letter topic whose creation is deferred until the first failure is a
     * dead-letter topic that does not exist yet.
     */
    @Bean
    NewTopic binanceRawTradesDltTopic(KafkaPipelineProperties properties) {
        KafkaPipelineProperties.Topics topics = properties.topics();
        return TopicBuilder.name(topics.rawTradesDlt())
                .partitions(topics.partitions())
                .replicas(topics.replicas())
                .configs(Map.of(
                        TopicConfig.RETENTION_MS_CONFIG,
                        Long.toString(topics.retention().multipliedBy(7).toMillis()),
                        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE))
                .build();
    }
}
