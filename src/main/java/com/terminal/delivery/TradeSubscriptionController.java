package com.terminal.delivery;

import com.terminal.ingestion.TradeEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.annotation.SubscribeMapping;
import org.springframework.stereotype.Controller;

import java.util.List;

/**
 * Serves the "what happened before you connected" half of the feed.
 *
 * <p>A live-only topic means a freshly opened tab shows an empty screen until the next
 * trade arrives — and on a quiet symbol that can be seconds, which reads as "it's
 * broken". The ring buffer turns that into an instant snapshot.
 *
 * <h2>Why {@code @SubscribeMapping} and not a REST endpoint</h2>
 * <p>The return value is delivered <b>to the subscribing session only</b>, on the
 * subscription's own reply destination — not broadcast to the {@code /topic} fan-out.
 * One connection, one round trip, no second HTTP call, and no way for client A to
 * receive client B's snapshot. The client subscribes to {@code /app/trades}
 * (application prefix from {@code terminal.delivery.websocket.application-prefix}),
 * receives one JSON array, and is done.
 *
 * <p>The method is synchronous and allocation-light: one {@code AtomicLong} read plus
 * {@code capacity} volatile reads. No I/O, so it cannot occupy a broker thread.
 */
@Slf4j
@Controller
public class TradeSubscriptionController {

    private final TradeRingBuffer ringBuffer;

    public TradeSubscriptionController(TradeRingBuffer ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    /**
     * @return the most recent trades, newest first. Serialized by the configured
     *         {@code MappingJackson2MessageConverter} exactly like the live messages,
     *         so the client can render both with the same code path.
     */
    @SubscribeMapping("/trades")
    public List<TradeEvent> snapshot() {
        List<TradeEvent> trades = ringBuffer.snapshot();
        log.debug("Snapshot served: {} of {} capacity ({} writes total)",
                trades.size(), ringBuffer.capacity(), ringBuffer.appended());
        return trades;
    }
}
