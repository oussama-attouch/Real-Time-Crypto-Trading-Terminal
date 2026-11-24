package com.terminal.orders;

import com.terminal.config.KafkaPipelineProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.UUID;

/**
 * Mock order engine: persist the intent, then announce it.
 *
 * <h2>Why the Kafka publish is not inside {@code placeOrder}</h2>
 * <p>The obvious implementation — {@code save(); kafkaTemplate.send()} in one method — can
 * publish an event for an order that then fails to commit, so downstream consumers would
 * see a phantom order. Spring's transaction-aware event listener fixes the ordering: the
 * event is published only in {@code AFTER_COMMIT}, so a rolled-back transaction produces
 * no Kafka record at all.
 *
 * <h2>What this still does not solve (and what would)</h2>
 * <p>It is a dual write, and {@code AFTER_COMMIT} narrows the window rather than closing
 * it: if the process dies between the commit and the send, or the broker is down, the
 * order exists in Postgres and no event exists in Kafka. The real fix is the
 * <b>transactional outbox</b> — insert the event into an {@code outbox} table in the same
 * transaction, then have a relay publish and mark it sent. That gives at-least-once
 * delivery with no phantom events, and it is the natural next step if this moves beyond a
 * mock engine.
 *
 * <h2>Postgres down</h2>
 * <p>Nothing special is done here: the failure surfaces as a transaction/connection
 * exception and {@link OrderExceptionHandler} turns it into {@code 503 Service Unavailable}
 * with {@code Retry-After}, which is the honest answer — the client should retry, not
 * treat it as a bad request.
 */
@Slf4j
@Service
public class OrderService {

    private final OrderRepository repository;
    private final ApplicationEventPublisher events;
    private final KafkaTemplate<String, OrderPlacedEvent> orderTemplate;
    private final String topic;

    public OrderService(OrderRepository repository,
                        ApplicationEventPublisher events,
                        KafkaTemplate<String, OrderPlacedEvent> orderTemplate,
                        KafkaPipelineProperties kafkaProperties) {
        this.repository = repository;
        this.events = events;
        this.orderTemplate = orderTemplate;
        this.topic = kafkaProperties.topics().orders();
    }

    /**
     * Persists the order with status {@code NEW} and announces it after commit.
     *
     * @throws org.springframework.transaction.CannotCreateTransactionException when the
     *         database is unreachable — mapped to 503 by {@link OrderExceptionHandler}
     */
    @Transactional
    public PlaceOrderResponse placeOrder(PlaceOrderRequest request) {
        Order order = new Order(
                UUID.randomUUID(),
                request.symbol().toUpperCase(Locale.ROOT),
                request.side(),
                // null stays null: that is a MARKET order, not a missing value.
                request.price(),
                request.quantity(),
                Order.OrderStatus.NEW,
                OffsetDateTime.now(ZoneOffset.UTC));

        Order saved = repository.save(order);

        // Not sent here: dispatched to the AFTER_COMMIT listener below, so the event can
        // never describe an order that was rolled back.
        events.publishEvent(OrderPlacedEvent.from(saved));

        log.info("Order accepted id={} {} {} qty={} @ {}", saved.getId(), saved.getSide(),
                saved.getSymbol(), saved.getQuantity().toPlainString(),
                saved.getPrice() == null ? "MARKET" : saved.getPrice().toPlainString());

        return PlaceOrderResponse.from(saved);
    }

    /**
     * Runs only after the surrounding transaction commits. Fire-and-observe: a broker
     * failure must not fail the HTTP request, because the order itself is already durable
     * and telling the client "failed" would invite a duplicate retry.
     */
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void publishOrderPlaced(OrderPlacedEvent event) {
        try {
            // Symbol as key: order events for one symbol stay ordered on one partition,
            // exactly like trades and depth.
            orderTemplate.send(topic, event.symbol(), event)
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            log.error("Order event publish FAILED orderId={} -> {}",
                                    event.orderId(), describe(error));
                            return;
                        }
                        log.debug("Order event published orderId={} topic={}", event.orderId(), topic);
                    });
        } catch (RuntimeException ex) {
            // send() can throw synchronously when max.block.ms expires (broker down).
            log.error("Order event publish REJECTED orderId={} -> {}", event.orderId(), describe(ex));
        }
    }

    private static String describe(Throwable error) {
        if (error == null) {
            return "unknown";
        }
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + (root.getMessage() == null ? "" : ": " + root.getMessage());
    }
}
