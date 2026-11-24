package com.terminal.orders;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * Reads {@code order-events} back off Kafka and fans it out to {@code /topic/orders}.
 *
 * <p>Why go through Kafka at all rather than pushing straight to STOMP from
 * {@link OrderService}? Because this is the CQRS seam: the write model commits to Postgres
 * and emits an event; the read model (this consumer, and anything else added later) reacts
 * to the log. A second consumer — an audit trail, a position keeper, a real matching
 * engine — can be added without touching the command path, and it can replay.
 *
 * <p>The symbol arrives as the record key (the producer keys by symbol), so the STOMP
 * broadcast carries the same ordering guarantee the topic does.
 */
@Slf4j
@Component
public class OrderKafkaConsumer {

    private final SimpMessagingTemplate messagingTemplate;
    private final String destination;

    public OrderKafkaConsumer(SimpMessagingTemplate messagingTemplate,
                              @Value("${terminal.orders.destination:/topic/orders}") String destination) {
        this.messagingTemplate = messagingTemplate;
        this.destination = destination;
    }

    /**
     * Uses {@code orderListenerContainerFactory}: the auto-configured container deserializes
     * into {@code TradeEvent}, which would silently swallow this payload's fields.
     */
    @KafkaListener(
            id = "order-events-consumer",
            topics = "${terminal.kafka.topics.orders:order-events}",
            groupId = "terminal-order-events-group",
            containerFactory = "orderListenerContainerFactory")
    public void onOrderPlaced(OrderPlacedEvent event) {
        try {
            messagingTemplate.convertAndSend(destination, event);
            if (log.isDebugEnabled()) {
                log.debug("Order event broadcast orderId={} {} {}", event.orderId(), event.side(), event.status());
            }
        } catch (Exception ex) {
            // A fan-out failure must not leave the listener (no poison loop for a UI push).
            log.error("Failed to broadcast order event {}: {}", event.orderId(), ex.toString());
        }
    }
}
