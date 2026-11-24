package com.terminal.aggregation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One finalized (or in-progress) 1-minute OHLCV candle.
 *
 * <p>Every price and volume field is a {@code String}, deliberately. The values come
 * from Binance as decimal strings ({@code "62950.73000000"}); once they have been
 * compared and summed exactly, they are emitted back as decimal strings so that no
 * consumer — Kafka topic, WebSocket frame, JavaScript chart — ever round-trips them
 * through a binary float.
 *
 * <p>{@code closeTime} is the inclusive last millisecond of the window
 * ({@code openTime + 59_999}), matching the convention of exchange candle APIs.
 *
 * @param openTime   epoch millis of the first millisecond in the window (a multiple of 60 000)
 * @param tradeCount number of trades aggregated, so a sparse candle is distinguishable
 *                   from a liquid one with the same volume
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CandleEvent(

        /** e.g. {@code BTCUSDT}; also the Kafka key, which is what keeps candle order per symbol. */
        @JsonProperty("symbol")
        String symbol,

        @JsonProperty("openTime")
        long openTime,

        @JsonProperty("closeTime")
        long closeTime,

        /** First trade price of the window. */
        @JsonProperty("open")
        String open,

        /** Highest trade price of the window. */
        @JsonProperty("high")
        String high,

        /** Lowest trade price of the window. */
        @JsonProperty("low")
        String low,

        /** Last trade price of the window. */
        @JsonProperty("close")
        String close,

        /** Summed traded quantity, exact decimal (never a float). */
        @JsonProperty("volume")
        String volume,

        @JsonProperty("tradeCount")
        long tradeCount) {

    /** True when this snapshot covers a complete window rather than an in-progress one. */
    public boolean isClosedAt(long nowMillis) {
        return nowMillis > closeTime;
    }
}
