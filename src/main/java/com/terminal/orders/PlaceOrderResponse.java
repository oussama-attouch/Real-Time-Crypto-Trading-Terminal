package com.terminal.orders;

import java.util.UUID;

/**
 * What the client gets back from {@code POST /api/orders}.
 *
 * <p>{@code status} is {@code NEW} today — with no matching engine, an accepted order is
 * simply "resting". {@code createdAt} is epoch millis, consistent with every other
 * timestamp on the wire.
 */
public record PlaceOrderResponse(
        UUID orderId,
        String symbol,
        String side,
        String price,
        String quantity,
        String status,
        long createdAt) {

    public static PlaceOrderResponse from(Order order) {
        return new PlaceOrderResponse(
                order.getId(),
                order.getSymbol(),
                order.getSide().name(),
                order.getPrice() == null ? "" : order.getPrice().toPlainString(),
                order.getQuantity().toPlainString(),
                order.getStatus().name(),
                order.getCreatedAt().toInstant().toEpochMilli());
    }
}
