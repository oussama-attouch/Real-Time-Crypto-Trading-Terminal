package com.terminal.aggregation;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Kafka -&gt; STOMP bridge for finalized candles.
 *
 * <p>Separate group id from {@code TradeKafkaConsumer} and from
 * {@code TradeAggregator}: independent consumers, each getting the full stream. Sharing a
 * group would split partitions instead of fanning out.
 *
 * <p>Like the trade consumer, this is synchronous but non-blocking:
 * {@code convertAndSend} serializes once and hands the frame to the outbound channel,
 * whose writes happen on its own pool. The listener thread never waits on a socket.
 */
@Slf4j
@Component
public class CandleKafkaConsumer {

    private final SimpMessagingTemplate messagingTemplate;
    private final String destination;
    private final Counter broadcast;

    public CandleKafkaConsumer(SimpMessagingTemplate messagingTemplate,
                              @Value("${terminal.delivery.websocket.candle-topic:/topic/klines}")
                              String destination,
                              MeterRegistry meterRegistry) {
        this.messagingTemplate = messagingTemplate;
        this.destination = destination;
        this.broadcast = Counter.builder("terminal.delivery.candles.broadcast")
                .description("Finalized candles handed to the STOMP broker").register(meterRegistry);
    }

    @KafkaListener(
            id = "candle-delivery",
            topics = "${terminal.kafka.topics.klines-1m:binance-klines-1m}",
            groupId = "terminal-candle-delivery-group",
            containerFactory = "candleListenerContainerFactory")
    public void onCandle(CandleEvent candle) {
        try {
            messagingTemplate.convertAndSend(destination, candle);
            broadcast.increment();
            if (log.isDebugEnabled()) {
                log.debug("Candle broadcast {} @{} C={} V={} -> {}",
                        candle.symbol(), candle.openTime(), candle.close(), candle.volume(), destination);
            }
        } catch (Exception ex) {
            // A broadcast failure must not leave the listener (no poison loop).
            log.error("Failed to broadcast candle {} @{}: {}",
                    candle.symbol(), candle.openTime(), ex.toString());
        }
    }
}
