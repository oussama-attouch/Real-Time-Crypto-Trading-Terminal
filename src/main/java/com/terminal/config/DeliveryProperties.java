package com.terminal.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.List;

/**
 * Typed configuration for the STOMP delivery side ({@code terminal.delivery.websocket.*}).
 *
 * <p>These values were declared back in Step 1 while the delivery code did not exist
 * yet. Binding them here is what keeps them from becoming phantom config: every key
 * below is actually applied to the endpoint or the outbound channel.
 *
 * @param endpoint         SockJS/STOMP handshake path, e.g. {@code /ws-trades}. This is
 *                         <b>not</b> where trades arrive — it is where the transport is
 *                         established; messages are then published to
 *                         {@link #tradeTopic()}.
 * @param applicationPrefix prefix for {@code @MessageMapping} methods (client -> server)
 * @param brokerPrefix     prefix the simple broker serves; the broker only accepts
 *                         subscriptions under this prefix, which is how we avoid
 *                         inventing an open relay
 * @param tradeTopic       destination the consumer broadcasts to
 * @param allowedOriginPatterns CORS origin patterns for the handshake. {@code "*"} is
 *                         for local development only — anything else lets any site
 *                         drive a stream through a user's browser.
 * @param sendBufferSizeLimit per-session outbound buffer in bytes. A client that stops
 *                         draining (frozen tab, slept laptop) would otherwise grow the
 *                         JVM heap until OOM; past this the session is closed instead.
 * @param sendTimeLimit    how long one message may sit in a session's buffer before the
 *                         session is considered stuck and closed
 * @param outboundPoolSize threads dedicated to writing to clients, so a slow browser
 *                         cannot stall the Kafka consumer thread
 * @param outboundQueueCapacity bound on queued outbound messages
 * @param heartbeat        STOMP heartbeat interval, both directions. Without it a
 *                         half-open TCP connection (slept laptop, NAT rebind, dead
 *                         proxy) looks perfectly healthy: no error, no close, just a
 *                         silent feed, and the browser cannot distinguish "quiet
 *                         market" from "dead socket". Set to 0 to disable.
 */
@Validated
@ConfigurationProperties(prefix = "terminal.delivery.websocket")
public record DeliveryProperties(

        @NotBlank String endpoint,

        @NotBlank String applicationPrefix,

        @NotBlank String brokerPrefix,

        @NotBlank String tradeTopic,

        @NotEmpty List<String> allowedOriginPatterns,

        @Min(1024) int sendBufferSizeLimit,

        @Min(1000) int sendTimeLimit,

        @Min(1) int outboundPoolSize,

        @Min(1) int outboundQueueCapacity,

        @NotNull Duration heartbeat) {
}
