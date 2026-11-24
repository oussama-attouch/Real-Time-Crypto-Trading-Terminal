package com.terminal.orders;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

/**
 * The event published to {@code order-events} once an order is committed.
 *
 * <p>Every decimal is a {@code String}, for the same reason as everywhere else in this
 * pipeline: it is the one representation that survives Java, JSON, JavaScript and Postgres
 * NUMERIC without a rounding step. {@code price} is empty (not null) for a MARKET order so
 * the payload shape is stable.
 *
 * <p>{@code timestamp} is epoch millis, matching {@code TradeEvent} and {@code CandleEvent}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderPlacedEvent(

        @JsonProperty("orderId") UUID orderId,

        @JsonProperty("symbol") String symbol,

        @JsonProperty("side") String side,

        @JsonProperty("price") String price,

        @JsonProperty("quantity") String quantity,

        @JsonProperty("status") String status,

        @JsonProperty("timestamp") long timestamp) {

    public static OrderPlacedEvent from(Order order) {
        return new OrderPlacedEvent(
                order.getId(),
                order.getSymbol(),
                order.getSide().name(),
                order.getPrice() == null ? "" : order.getPrice().toPlainString(),
                order.getQuantity().toPlainString(),
                order.getStatus().name(),
                order.getCreatedAt().toInstant().toEpochMilli());
    }
}
