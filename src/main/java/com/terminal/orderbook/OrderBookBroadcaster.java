package com.terminal.orderbook;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Pushes the latest real L2 book to {@code /topic/orderbook} at a fixed 10 Hz.
 *
 * <h2>Why sampling and not per-frame fan-out</h2>
 * <p>{@link DepthKafkaConsumer} receives ~10 frames/s per symbol. Forwarding each one
 * would mean serializing and fanning out a full 40-level book ten times a second to every
 * connected browser, and the browser can only paint ~60 frames/s in total — most of that
 * traffic would be thrown away on arrival.
 *
 * <p>Sampling decouples the two: the consumer only replaces a map entry (microseconds,
 * no serialization), and one scheduled thread emits at most 10 snapshots/s per symbol
 * whatever the feed does. The UI is indistinguishable, and the cost is bounded by wall
 * time instead of by market activity.
 *
 * <p>Frames are also skipped when the depth version has not changed, so a quiet market
 * costs nothing at all.
 */
@Slf4j
@Component
public class OrderBookBroadcaster {

    private final DepthKafkaConsumer depthConsumer;
    private final SimpMessagingTemplate messagingTemplate;
    private final Clock clock;
    private final String destination;
    private final int depth;
    private final Counter broadcastCounter;

    /** Last version pushed per symbol; suppresses identical frames. */
    private final Map<String, Long> lastVersions = new ConcurrentHashMap<>();

    public OrderBookBroadcaster(DepthKafkaConsumer depthConsumer,
                                SimpMessagingTemplate messagingTemplate,
                                Clock clock,
                                @Value("${terminal.orderbook.destination:/topic/orderbook}") String destination,
                                @Value("${terminal.orderbook.depth:20}") int depth,
                                MeterRegistry meterRegistry) {
        this.depthConsumer = depthConsumer;
        this.messagingTemplate = messagingTemplate;
        this.clock = clock;
        this.destination = destination;
        this.depth = depth;
        this.broadcastCounter = Counter.builder("terminal.orderbook.snapshots.broadcast")
                .description("Order book snapshots handed to the STOMP broker")
                .register(meterRegistry);
    }

    /**
     * 10 Hz. Keep the interval at or above ~50 ms: below that the snapshot serialization
     * starts to rival the frame rate it exists to smooth out.
     */
    @Scheduled(fixedRateString = "${terminal.orderbook.broadcast-interval-ms:100}")
    public void broadcast() {
        long now = clock.millis();

        for (Map.Entry<String, DepthKafkaConsumer.LatestDepth> entry : depthConsumer.latestFrames().entrySet()) {
            String symbol = entry.getKey();
            DepthKafkaConsumer.LatestDepth latest = entry.getValue();

            Long previous = lastVersions.get(symbol);
            if (previous != null && previous == latest.version()) {
                continue;   // no new depth frame since the last broadcast
            }

            OrderBookSnapshot snapshot = OrderBookSnapshot.from(symbol, latest.depth(), depth, now);
            try {
                messagingTemplate.convertAndSend(destination, snapshot);
                lastVersions.put(symbol, latest.version());
                broadcastCounter.increment();
            } catch (Exception ex) {
                // Must not escape: an exception here makes the scheduler cancel every
                // future execution of this method.
                log.error("Failed to broadcast order book for {}: {}", symbol, ex.toString());
            }
        }
    }
}
