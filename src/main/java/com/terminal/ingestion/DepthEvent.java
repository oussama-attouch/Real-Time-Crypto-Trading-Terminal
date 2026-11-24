package com.terminal.ingestion;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * One partial depth snapshot from Binance's {@code @depth20@100ms} stream.
 *
 * <p>Verified live payload shape (spot, combined stream):
 * <pre>
 * {"stream":"btcusdt@depth20@100ms",
 *  "data":{"lastUpdateId":103614277455,
 *          "bids":[["59936.31000000","2.47881000"], ...],
 *          "asks":[["59936.32000000","0.39034000"], ...]}}
 * </pre>
 *
 * <h2>Two things this record cannot carry, and where they come from instead</h2>
 * <ul>
 *   <li><b>There is no symbol field.</b> Binance omits it from the partial-depth payload
 *       (unlike {@code aggTrade}, which has {@code s}). The symbol is derived from the
 *       stream name at ingestion and becomes the Kafka message key, so the consumer
 *       recovers it from {@code ConsumerRecord.key()} — which is also what keeps depth
 *       ordered per symbol, exactly like trades.</li>
 *   <li><b>No {@code E}/{@code T} timestamps</b> on the spot partial stream, so freshness
 *       is measured by the consumer's receipt time.</li>
 * </ul>
 *
 * <p>Levels stay as raw {@code List<List<String>>} because that is the wire shape and it
 * is lossless: {@code [["price","quantity"], ...]}. Parsing to {@code BigDecimal} happens
 * once, in {@code OrderBookSnapshot.from(...)}, where the numbers are actually needed.
 * Treating them as anything but strings before that point is how precision gets lost.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DepthEvent(

        /** Monotonic id of the snapshot; a later frame always supersedes an earlier one. */
        @JsonProperty("lastUpdateId") long lastUpdateId,

        /** {@code [price, quantity]} pairs, best (highest) price first as sent. */
        @JsonProperty("bids") List<List<String>> bids,

        /** {@code [price, quantity]} pairs, best (lowest) price first as sent. */
        @JsonProperty("asks") List<List<String>> asks) {

    public DepthEvent {
        // Normalise absent sides so downstream code never has to null-check.
        bids = bids == null ? List.of() : bids;
        asks = asks == null ? List.of() : asks;
    }
}
