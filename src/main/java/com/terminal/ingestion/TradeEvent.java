package com.terminal.ingestion;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One aggregated trade from Binance.
 *
 * <p>Wire size is 6 fields on purpose. Two deliberate decisions:
 *
 * <h3>Everything numeric is a String or a long — never a floating-point type</h3>
 * <p>Binance sends {@code "p":"62950.73000000"}. Parsing that into {@code double}
 * loses bits ({@code 0.1 + 0.2 != 0.3}), and — the real trap — JavaScript parses
 * JSON numbers into IEEE-754 doubles, so a 19-digit {@code tradeId} silently
 * becomes a different number in the browser. Keeping {@code price},
 * {@code quantity} and {@code tradeId} as {@code String} is lossless from the
 * exchange to the chart. No {@code BigDecimal}, no {@code double}, no rounding to
 * argue about later.
 *
 * <h3>Binance's field names stop at this boundary</h3>
 * <p>Binance names its fields {@code a, s, p, q, T, m}. Our internal contract is
 * {@code tradeId, symbol, price, quantity, tradeTime, isBuyerMaker}. {@link
 * JsonAlias} gives us both directions from one field: it is a <em>read-side
 * only</em> alias, so Binance's short names are accepted on the way in, while
 * everything we emit (Kafka payload, STOMP frame) uses the readable names that the
 * React client and any human tailing the topic can actually understand. Without
 * this, {@code a}/{@code p}/{@code q} would leak into the frontend and into every
 * downstream service as an undocumented dialect.
 *
 * <p>{@link JsonProperty} is spelled out on every component even where it matches
 * the component name: for records, an accessor like {@code isBuyerMaker()} also
 * looks like an {@code is}-getter for a property called {@code buyerMaker}, and
 * pinning the name removes that ambiguity rather than leaving it to Jackson's
 * introspection heuristics.
 *
 * <p>{@code ignoreUnknown = true} matters: real frames carry extra fields
 * ({@code e}, {@code E}, {@code f}, {@code l}, {@code M}), and a schema addition on
 * Binance's side must never kill the ingestion stream.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TradeEvent(

        /** Binance {@code a}. String: 19-digit ids lose precision in JS numbers. */
        @JsonProperty("tradeId")
        @JsonAlias("a")
        String tradeId,

        /** Binance {@code s}, e.g. {@code BTCUSDT}. Used as the Kafka message key. */
        @JsonProperty("symbol")
        @JsonAlias("s")
        String symbol,

        /** Binance {@code p}, e.g. {@code "62950.73000000"}. Exact, as sent. */
        @JsonProperty("price")
        @JsonAlias("p")
        String price,

        /** Binance {@code q}, e.g. {@code "0.00160000"}. Exact, as sent. */
        @JsonProperty("quantity")
        @JsonAlias("q")
        String quantity,

        /** Binance {@code T}: trade time, epoch millis. */
        @JsonProperty("tradeTime")
        @JsonAlias("T")
        long tradeTime,

        /** Binance {@code m}: true when the buyer was the maker (aggressor sold). */
        @JsonProperty("isBuyerMaker")
        @JsonAlias("m")
        boolean isBuyerMaker) {
}
