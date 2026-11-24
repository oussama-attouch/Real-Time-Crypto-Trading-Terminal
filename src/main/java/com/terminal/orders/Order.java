package com.terminal.orders;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * A persisted order intent.
 *
 * <p>A JPA entity, not a record: JPA needs a mutable class with a no-arg constructor, so
 * the immutable-record style used everywhere else in this codebase stops at the
 * persistence boundary. Everything crossing that boundary in the other direction
 * ({@link PlaceOrderRequest}, {@link PlaceOrderResponse}, {@link OrderPlacedEvent}) is a
 * record.
 *
 * <p>{@code price} is nullable by design: NULL means a MARKET order, where there is no
 * limit price to record. Everything monetary is {@link BigDecimal} — never a float, never
 * a {@code double}, in any column or field.
 */
@Entity
@Table(name = "orders")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)   // for JPA only
public class Order {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "symbol", nullable = false, length = 32)
    private String symbol;

    @Enumerated(EnumType.STRING)
    @Column(name = "side", nullable = false, length = 8)
    private Side side;

    /** NULL = MARKET order. */
    @Column(name = "price", precision = 38, scale = 18)
    private BigDecimal price;

    @Column(name = "quantity", nullable = false, precision = 38, scale = 18)
    private BigDecimal quantity;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    private OrderStatus status;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    Order(UUID id, String symbol, Side side, BigDecimal price, BigDecimal quantity,
          OrderStatus status, OffsetDateTime createdAt) {
        this.id = id;
        this.symbol = symbol;
        this.side = side;
        this.price = price;
        this.quantity = quantity;
        this.status = status;
        this.createdAt = createdAt;
    }

    /** Only transitions a mock engine needs today; FILLED/CANCELLED land in Phase 9. */
    void markFilled() {
        this.status = OrderStatus.FILLED;
    }

    void cancel() {
        this.status = OrderStatus.CANCELLED;
    }

    public enum Side {
        BUY,
        SELL
    }

    public enum OrderStatus {
        NEW,
        FILLED,
        CANCELLED
    }
}
