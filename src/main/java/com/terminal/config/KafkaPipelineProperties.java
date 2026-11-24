package com.terminal.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Kafka plumbing owned by the ingestion side ({@code terminal.kafka.*}).
 *
 * <p>Note what is <em>not</em> here: bootstrap servers, serializers, acks,
 * idempotence, {@code max.block.ms}. Those live in {@code spring.kafka.*} and are
 * consumed by Boot's auto-configured {@code ProducerFactory}/{@code KafkaTemplate}.
 * Duplicating them here would create two sources of truth for the producer.
 *
 * @param topics topic names and physical layout
 */
@Validated
@ConfigurationProperties(prefix = "terminal.kafka")
public record KafkaPipelineProperties(

        @Valid @NotNull Topics topics) {

    /**
     * @param rawTrades    names the source of truth for the whole pipeline; the
     *                     delivery service (Step 3) binds the same property
     * @param rawTradesDlt dead-letter topic for records the consumer cannot handle
     * @param klines1m     finalized 1-minute candles produced by the aggregator
     * @param depth        partial L2 order book snapshots straight from Binance
     *                     ({@code @depth20@100ms}); keyed by symbol like everything else
     * @param partitions   see {@code application.yml} — 6 is headroom for symbol
     *                     fan-out, not for splitting one symbol
     * @param replicas     must be 1 on the single-broker dev cluster
     * @param retention    replay window
     */
    public record Topics(
            @NotBlank String rawTrades,
            @NotBlank String rawTradesDlt,
            @NotBlank String klines1m,
            @NotBlank String depth,
            @NotBlank String orders,
            @Min(1) int partitions,
            @Min(1) short replicas,
            @NotNull Duration retention) {
    }
}
