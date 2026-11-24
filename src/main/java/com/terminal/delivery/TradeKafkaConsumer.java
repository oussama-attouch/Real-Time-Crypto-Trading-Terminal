package com.terminal.delivery;

import com.terminal.config.DeliveryProperties;
import com.terminal.ingestion.TradeEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * Kafka -&gt; STOMP bridge.
 *
 * <h2>Why this is non-blocking even though it is not reactive</h2>
 * <p>{@code @KafkaListener} methods run on the listener container's own thread
 * ({@code kafka-listener-*}), never on a Netty event loop — there is no event loop in
 * this code path at all. {@link SimpMessagingTemplate#convertAndSend} then does exactly
 * two things synchronously: serialize the payload once, and hand the resulting message
 * to the {@code clientOutboundChannel}, which is a separate executor (configured in
 * {@link WebSocketConfig#configureClientOutboundChannel}). The actual socket writes
 * happen on that pool. So the consumer thread pays serialization plus a queue insert —
 * O(1) per message, independent of how many clients are connected and of how slowly
 * any one of them drains.
 *
 * <p>That is also why no {@code publishOn}/{@code subscribeOn} belongs here: adding a
 * scheduler hop to a call that neither blocks nor performs I/O would only add latency
 * and hand-off cost.
 */
@Slf4j
@Component
public class TradeKafkaConsumer {

    private final SimpMessagingTemplate messagingTemplate;
    private final DeliveryProperties properties;
    private final TradeRingBuffer ringBuffer;
    private final Counter broadcast;

    public TradeKafkaConsumer(SimpMessagingTemplate messagingTemplate,
                              DeliveryProperties properties,
                              TradeRingBuffer ringBuffer,
                              MeterRegistry meterRegistry) {
        this.messagingTemplate = messagingTemplate;
        this.properties = properties;
        this.ringBuffer = ringBuffer;
        this.broadcast = Counter.builder("terminal.delivery.trades.broadcast")
                .description("Trades handed to the STOMP broker for fan-out")
                .register(meterRegistry);
        // Same singleton the controller snapshots from: what a new subscriber receives
        // is exactly what has flowed through this method.
        Gauge.builder("terminal.delivery.snapshot.size", ringBuffer, TradeRingBuffer::size)
                .description("Trades currently cached for new-subscriber snapshots")
                .register(meterRegistry);
    }

    /**
     * Consumes the ingestion topic and fans out to every subscriber of
     * {@code /topic/trades}.
     *
     * <p>{@code groupId} is pinned literally here rather than left to
     * {@code spring.kafka.consumer.group-id}: with a single delivery listener an
     * implicit default is a trap waiting to be copy-pasted into a second listener, at
     * which point two consumers would silently share partitions.
     */
    @KafkaListener(
            id = "trade-delivery",
            topics = "${terminal.kafka.topics.raw-trades:binance-raw-trades}",
            groupId = "terminal-delivery-group")
    public void onTrade(TradeEvent trade) {
        // Cache BEFORE broadcasting: a client connecting in the same instant must not
        // get a snapshot that is missing the trade the live topic is already sending.
        // A ring-buffer write is two atomics and cannot fail or block.
        ringBuffer.add(trade);
        try {
            messagingTemplate.convertAndSend(properties.tradeTopic(), trade);
            broadcast.increment();
            if (log.isDebugEnabled()) {
                log.debug("Broadcast {} {} qty={} -> {}",
                        trade.symbol(), trade.price(), trade.quantity(), properties.tradeTopic());
            }
        } catch (Exception ex) {
            // Deliberately broad: a broadcast failure must never propagate out of the
            // listener. Throwing here would make the container treat the record as
            // failed, seek back and redeliver it — a poison loop for a message whose
            // only consumer is a transient browser socket. The next tick matters more.
            log.error("Failed to broadcast {} id={} to {}: {}",
                    trade.symbol(), trade.tradeId(), properties.tradeTopic(), ex.toString());
        }
    }
}
