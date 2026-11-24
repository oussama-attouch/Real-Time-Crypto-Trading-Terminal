package com.terminal.ingestion;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Binance combined-stream envelope: {@code {"stream": "...", "data": { ... }}}.
 *
 * <p>One socket now carries two different {@code data} shapes, selected by the stream name:
 * <pre>
 *   btcusdt@aggTrade            -> TradeEvent  {"e":"aggTrade","s":"BTCUSDT","p":...}
 *   btcusdt@depth20@100ms       -> DepthEvent  {"lastUpdateId":...,"bids":[[...]],"asks":[[...]]}
 * </pre>
 * so {@code data} is held as a {@link JsonNode} and bound to the right record once the
 * stream name has been read. Cost: one extra binding pass per frame versus deserializing
 * straight into a concrete type. At ~25 trades/s plus ~10 depth frames/s that is
 * microseconds, and it is the only way both feeds can share one socket.
 *
 * <p>{@code ignoreUnknown = true}: control frames such as {@code {"result":null,"id":1}}
 * bind to {@code data == null} and are filtered by the client rather than treated as errors.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BinanceStreamEnvelope(String stream, JsonNode data) {
}
