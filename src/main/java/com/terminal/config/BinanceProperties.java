package com.terminal.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Typed, validated configuration for the Binance ingestion pipeline
 * ({@code terminal.binance.*}).
 *
 * <p>Deliberately no defaults baked in for anything structural: {@code @NotNull}
 * plus {@code @Validated} makes a missing key fail <em>at startup</em> with the
 * property name in the message, which is far better than a null blowing up inside
 * a Reactor operator at 3am. {@code application.yml} stays the single source of
 * truth.
 *
 * @param enabled              kill switch; {@code false} keeps the socket closed
 *                             (also used by tests that do not want live traffic)
 * @param url                  combined-stream endpoint, plural {@code /stream?streams=}
 * @param connectTimeout       TCP connect timeout
 * @param handshakeTimeout     how long to wait for the HTTP 101 upgrade response
 * @param idleTimeout          staleness watchdog: no frame for this long forces a
 *                             reconnect. Guards against a silently dead socket that
 *                             the OS never reports as closed (NAT rebinding, etc.)
 * @param maxFramePayloadLength max WebSocket frame size; the Spring default of
 *                             64 KiB is tight for bursty streams
 * @param reconnect            exponential-backoff settings
 * @param scheduler            dedicated pool for the Kafka publish step
 */
@Validated
@ConfigurationProperties(prefix = "terminal.binance")
public record BinanceProperties(

        @DefaultValue("true") boolean enabled,

        @NotBlank String url,

        @NotNull Duration connectTimeout,

        @NotNull Duration handshakeTimeout,

        @NotNull Duration idleTimeout,

        @Min(1024) int maxFramePayloadLength,

        @Valid @NotNull Reconnect reconnect,

        @Valid @NotNull SchedulerSettings scheduler) {

    /**
     * @param initialBackoff delay before the first retry (= {@code Retry.backoff}
     *                       base)
     * @param maxBackoff     ceiling for the exponential growth
     * @param multiplier     documented only; Reactor's {@code Retry.backoff} doubles
     *                       per attempt, so a value other than 2.0 is not applied
     * @param jitterFactor   spread applied to each delay (0.5 = +/-25%). Mandatory in
     *                       practice: without jitter every replica retries in lockstep
     *                       after an outage and Binance rate-limits or bans the IP.
     * @param maxAttempts    0 = retry forever (mapped to {@code Long.MAX_VALUE}),
     *                       which is the right answer for a market-data feed: the only
     *                       alternatives are "retry" or "be wrong"
     */
    public record Reconnect(
            @NotNull Duration initialBackoff,
            @NotNull Duration maxBackoff,
            @DecimalMin("1.0") double multiplier,
            @DecimalMin("0.0") @DecimalMax("1.0") double jitterFactor,
            @Min(0) long maxAttempts) {
    }

    /**
     * @param threadPrefix  thread names, so logs prove which thread did the work
     * @param poolSize      hard cap on concurrently blocked publish threads
     * @param queueCapacity bound on queued publish tasks; past this the scheduler
     *                      rejects and Reactor surfaces backpressure instead of
     *                      growing an unbounded queue
     * @param prefetch      {@code publishOn} buffer size between the Netty read loop
     *                      and the publish stage. This is the real bounded buffer of
     *                      the pipeline: when Kafka slows down, this queue fills and
     *                      upstream requests stop, which propagates TCP
     *                      backpressure to Binance. Nothing is dropped silently and
     *                      nothing grows without bound.
     */
    public record SchedulerSettings(
            @NotBlank String threadPrefix,
            @Min(1) int poolSize,
            @Min(1) int queueCapacity,
            @Min(1) int prefetch) {
    }
}
