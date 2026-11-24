package com.terminal.orderbook;

import com.terminal.ingestion.DepthEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumes real L2 depth from {@code binance-depth} and keeps only the newest snapshot
 * per symbol.
 *
 * <p>This is the throttle's write side: Binance pushes ~10 frames/s per symbol, this
 * listener overwrites one map entry each time (no serialization, no fan-out), and
 * {@link OrderBookBroadcaster} samples that entry at its own pace. Doing the fan-out in
 * the listener instead would put the socket on the ingest path and send every frame to
 * every browser, most of which the browser could never paint.
 *
 * <h2>Why the symbol comes from the record key</h2>
 * <p>Binance's partial-depth payload has no symbol field (verified against the live feed:
 * {@code {"lastUpdateId":...,"bids":...,"asks":...}}), so the ingestion service derives it
 * from the stream name and uses it as the Kafka key. Reading {@code record.key()} here
 * recovers it, and because the key is also what partitions the topic, depth for one symbol
 * is always processed by one thread in order — a later snapshot can never be overwritten
 * by an earlier one.
 *
 * <p>{@code lastUpdateId} increases monotonically, so a stale frame could be detected and
 * dropped here. It is deliberately not enforced: with a single writer per symbol, Kafka
 * ordering already guarantees it, and a second check would only add a branch to the hot
 * path.
 */
@Slf4j
@Component
public class DepthKafkaConsumer {

    /** Newest snapshot for a symbol plus a monotonic version the broadcaster uses to skip no-ops. */
    public record LatestDepth(DepthEvent depth, long version) {
    }

    private final ConcurrentHashMap<String, LatestDepth> latest = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong();
    private final Counter received;

    public DepthKafkaConsumer(MeterRegistry meterRegistry) {
        this.received = Counter.builder("terminal.orderbook.depth.received")
                .description("L2 depth frames consumed from Kafka").register(meterRegistry);
    }

    @KafkaListener(
            id = "depth-consumer",
            topics = "${terminal.kafka.topics.depth:binance-depth}",
            groupId = "terminal-depth-group",
            containerFactory = "depthListenerContainerFactory")
    public void onDepth(ConsumerRecord<String, DepthEvent> record) {
        DepthEvent depth = record.value();
        String symbol = record.key();

        if (depth == null || symbol == null) {
            // Nothing to render and no way to attribute it; skip rather than poison the loop.
            log.warn("Skipping depth record with missing value or key (topic={}, partition={}, offset={})",
                    record.topic(), record.partition(), record.offset());
            return;
        }

        latest.put(symbol, new LatestDepth(depth, sequence.incrementAndGet()));
        received.increment();
        if (log.isTraceEnabled()) {
            log.trace("depth consumed {} lastUpdateId={} bids={} asks={}",
                    symbol, depth.lastUpdateId(), depth.bids().size(), depth.asks().size());
        }
    }

    /** Live view used by the broadcaster; iteration is weakly consistent, which is fine here. */
    public Map<String, LatestDepth> latestFrames() {
        return latest;
    }
}
