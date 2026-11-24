package com.terminal.orders;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;

/**
 * Inbound order intent.
 *
 * <p>Decimals arrive as JSON <b>strings</b> ({@code "price":"60000.00"}) exactly as they
 * do on the market-data side, and bind to {@link BigDecimal}. Jackson would happily accept
 * a JSON number too, but the client never sends one — that is the whole point of the
 * string convention used across this project.
 *
 * <p>{@code price} is nullable: absent means MARKET. {@code @DecimalMin(inclusive=false)}
 * on a null value passes validation (null is only rejected by {@code @NotNull}), so the
 * rule reads correctly for both order kinds: "if a price is supplied, it must be
 * positive".
 *
 * @param side constrained by the same strings the DB CHECK constraint allows, so an
 *             invalid side is a 400 from validation rather than a 500 from Postgres
 */
public record PlaceOrderRequest(

        @NotBlank
        @Size(min = 5, max = 32)
        @Pattern(regexp = "^[A-Z0-9]+$", message = "symbol must be upper-case alphanumeric, e.g. BTCUSDT")
        String symbol,

        @NotNull
        Order.Side side,

        @DecimalMin(value = "0", inclusive = false, message = "price must be greater than 0")
        BigDecimal price,

        @NotNull
        @DecimalMin(value = "0", inclusive = false, message = "quantity must be greater than 0")
        BigDecimal quantity) {
}
