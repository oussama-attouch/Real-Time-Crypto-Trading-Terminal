package com.terminal.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.terminal.config.BinanceProperties;
import com.terminal.config.KafkaPipelineProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.Retry;

import java.net.URI;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

/**
 * Reactor-Netty WebSocket consumer for Binance, publishing trades and partial order book
 * depth to Kafka.
 *
 * <h2>Two feeds, one socket</h2>
 * <p>The combined endpoint carries {@code streams=btcusdt@aggTrade/btcusdt@depth20@100ms},
 * so each frame is dispatched on its stream name: {@code @aggTrade} becomes a
 * {@link TradeEvent} on {@code binance-raw-trades}, {@code @depth20} becomes a
 * {@link DepthEvent} on {@code binance-depth}. One connection means one set of pings, one
 * reconnect ladder, and both feeds moving in lockstep — two sockets would double the
 * connection count and could drift apart under reconnect.
 *
 * <h2>Threading contract</h2>
 * <p>{@code session.receive()} emits on a {@code reactor-http-nio-*} event-loop thread,
 * which serves every socket multiplexed onto it; a single blocking call there stalls them
 * all. {@code KafkaTemplate.send()} is exactly such a call — it blocks the caller for up
 * to {@code max.block.ms} while fetching metadata or waiting for send-buffer space — so
 * {@code publishOn(...)} moves all of it, JSON binding included, onto our bounded pool.
 * See {@link #handleSession(WebSocketSession)}.
 *
 * <h2>Failure policy</h2>
 * <p>Contained at both levels: a malformed frame is logged and dropped (it must not poison
 * the session), and a failed Kafka publish is logged at ERROR from the
 * {@link CompletableFuture} callback and dropped (it must not tear down the socket —
 * reconnecting costs far more than one stale frame).
 */
@Slf4j
@Component
public class BinanceWebSocketClient {

    private static final int LOG_FRAME_PREVIEW_CHARS = 240;
    private static final String TRADE_STREAM_MARKER = "@aggTrade";
    private static final String DEPTH_STREAM_MARKER = "@depth";

    private final ReactorNettyWebSocketClient webSocketClient;
    private final ObjectMapper jsonMapper;
    private final KafkaTemplate<String, TradeEvent> tradeTemplate;
    private final KafkaTemplate<String, DepthEvent> depthTemplate;
    private final BinanceProperties properties;
    private final Scheduler ingestScheduler;
    private final URI endpoint;
    private final String tradeTopic;
    private final String depthTopic;

    private final Counter framesReceived;
    private final Counter tradesParsed;
    private final Counter tradesPublished;
    private final Counter tradesFailed;
    private final Counter depthParsed;
    private final Counter depthFailed;

    public BinanceWebSocketClient(ReactorNettyWebSocketClient webSocketClient,
                                  ObjectMapper jsonMapper,
                                  KafkaTemplate<String, TradeEvent> tradeTemplate,
                                  KafkaTemplate<String, DepthEvent> depthTemplate,
                                  BinanceProperties properties,
                                  KafkaPipelineProperties kafkaProperties,
                                  @Qualifier("binanceIngestScheduler") Scheduler binanceIngestScheduler,
                                  MeterRegistry meterRegistry) {
        this.webSocketClient = webSocketClient;
        this.jsonMapper = jsonMapper;
        this.tradeTemplate = tradeTemplate;
        this.depthTemplate = depthTemplate;
        this.properties = properties;
        this.ingestScheduler = binanceIngestScheduler;
        this.endpoint = URI.create(properties.url());
        this.tradeTopic = kafkaProperties.topics().rawTrades();
        this.depthTopic = kafkaProperties.topics().depth();

        this.framesReceived = Counter.builder("terminal.ingestion.frames.received")
                .description("Raw WebSocket frames received from Binance").register(meterRegistry);
        this.tradesParsed = Counter.builder("terminal.ingestion.trades.parsed")
                .description("Frames decoded into a TradeEvent").register(meterRegistry);
        this.tradesPublished = Counter.builder("terminal.ingestion.trades.published")
                .description("Trades acknowledged by the Kafka broker").register(meterRegistry);
        this.tradesFailed = Counter.builder("terminal.ingestion.trades.failed")
                .description("Trades dropped because the Kafka publish failed").register(meterRegistry);
        this.depthParsed = Counter.builder("terminal.ingestion.depth.parsed")
                .description("Frames decoded into a DepthEvent").register(meterRegistry);
        this.depthFailed = Counter.builder("terminal.ingestion.depth.failed")
                .description("Depth frames dropped because the Kafka publish failed").register(meterRegistry);
    }

    /** Cold signal; nothing happens until {@code IngestionRunner} subscribes. */
    public Mono<Void> connect() {
        log.info("Binance ingestion starting :: endpoint={} trades->{} depth->{}",
                endpoint, tradeTopic, depthTopic);
        return openSession().retryWhen(reconnectBackoff());
    }

    private Mono<Void> openSession() {
        return Mono.defer(() -> {
                    log.info("Binance WS: opening connection -> {}", endpoint);
                    return webSocketClient.execute(endpoint, this::handleSession);
                })
                // A clean close is still an unexpected disconnect for a live feed; mapping
                // completion to an error lets ONE retry ladder cover both cases.
                .then(Mono.error(new SessionClosedException("WebSocket session closed by peer")));
    }

    private Retry reconnectBackoff() {
        BinanceProperties.Reconnect cfg = properties.reconnect();
        long maxAttempts = cfg.maxAttempts() <= 0 ? Long.MAX_VALUE : cfg.maxAttempts();
        return Retry.backoff(maxAttempts, cfg.initialBackoff())
                .maxBackoff(cfg.maxBackoff())
                .jitter(cfg.jitterFactor())
                .doBeforeRetry(signal -> log.warn(
                        "Binance WS: DISCONNECTED ({}); scheduling reconnect attempt #{} (backoff {}..{})",
                        describe(signal.failure()), signal.totalRetries() + 1,
                        cfg.initialBackoff(), cfg.maxBackoff()));
    }

    /**
     * <pre>
     * reactor-http-nio-*   receive -> getPayloadAsText -> timeout
     *        |
     *        |   publishOn(ingestScheduler, prefetch)   &lt;-- the one thread hop
     *        v
     * binance-ingest-*     dispatch(frame) -> bind -> kafkaTemplate.send()
     * </pre>
     *
     * <p>With two feeds there is no longer a single decoded element type to emit, so the
     * previous {@code handle(...) + doOnNext(publish)} pair collapses into one terminal
     * {@link #dispatch(String)} stage. Everything downstream of {@code publishOn} still runs
     * on the bounded ingest pool, so the event loop only reads and hands over.
     */
    private Mono<Void> handleSession(WebSocketSession session) {
        log.info("Binance WS: CONNECTED (sessionId={})", session.getId());

        return session.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .doOnNext(frame -> framesReceived.increment())
                // Staleness watchdog: a socket can look open while the peer is gone.
                .timeout(properties.idleTimeout())
                // ---- hop off the event loop; everything below is pool work ----
                .publishOn(ingestScheduler, properties.scheduler().prefetch())
                .doOnNext(this::dispatch)
                .then()
                .doFinally(signal -> log.warn("Binance WS: DISCONNECTED (signal={})", signal));
    }

    /** Unwraps the envelope and routes on the stream name. */
    private void dispatch(String rawFrame) {
        final BinanceStreamEnvelope envelope;
        try {
            envelope = jsonMapper.readValue(rawFrame, BinanceStreamEnvelope.class);
        } catch (JsonProcessingException ex) {
            log.warn("Dropping unparseable frame ({} chars): {} | {}",
                    rawFrame.length(), abbreviate(rawFrame), ex.getOriginalMessage());
            return;
        }

        JsonNode data = envelope.data();
        if (data == null || data.isNull()) {
            // Subscription acks such as {"result":null,"id":1}.
            log.trace("Ignoring non-data frame: {}", abbreviate(rawFrame));
            return;
        }

        String stream = envelope.stream();
        if (stream != null && stream.contains(DEPTH_STREAM_MARKER)) {
            publishDepth(data, stream, rawFrame);
        } else if (stream == null || stream.contains(TRADE_STREAM_MARKER)) {
            publishTrade(data, rawFrame);
        } else {
            log.trace("Ignoring frame from unhandled stream '{}'", stream);
        }
    }

    private void publishTrade(JsonNode data, String rawFrame) {
        final TradeEvent trade;
        try {
            trade = jsonMapper.treeToValue(data, TradeEvent.class);
        } catch (JsonProcessingException ex) {
            log.warn("Dropping unbindable trade frame: {} | {}", abbreviate(rawFrame), ex.getOriginalMessage());
            return;
        }

        if (trade.symbol() == null || trade.tradeId() == null
                || trade.price() == null || trade.quantity() == null) {
            log.warn("Dropping malformed aggTrade frame: {}", abbreviate(rawFrame));
            return;
        }

        tradesParsed.increment();
        if (log.isTraceEnabled()) {
            log.trace("aggTrade RX {} p={} q={} id={} buyerIsMaker={}",
                    trade.symbol(), trade.price(), trade.quantity(), trade.tradeId(), trade.isBuyerMaker());
        }

        try {
            tradeTemplate.send(tradeTopic, trade.symbol(), trade)
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            tradesFailed.increment();
                            log.error("Kafka publish FAILED trade {} id={} -> {}",
                                    trade.symbol(), trade.tradeId(), describe(error));
                            return;
                        }
                        tradesPublished.increment();
                        if (log.isDebugEnabled()) {
                            log.debug("Kafka ack trade {} id={}", trade.symbol(), trade.tradeId());
                        }
                    });
        } catch (RuntimeException ex) {
            // send() throws when max.block.ms expires (broker down / buffer full).
            tradesFailed.increment();
            log.error("Kafka publish REJECTED trade {} id={} -> {}",
                    trade.symbol(), trade.tradeId(), describe(ex));
        }
    }

    private void publishDepth(JsonNode data, String rawStream, String rawFrame) {
        final DepthEvent depth;
        try {
            depth = jsonMapper.treeToValue(data, DepthEvent.class);
        } catch (JsonProcessingException ex) {
            log.warn("Dropping unbindable depth frame: {} | {}", abbreviate(rawFrame), ex.getOriginalMessage());
            return;
        }

        if (depth.bids().isEmpty() && depth.asks().isEmpty()) {
            log.warn("Dropping empty depth frame (lastUpdateId={})", depth.lastUpdateId());
            return;
        }

        // Binance's partial-depth payload has no symbol field (verified against the live
        // feed), so the stream name is the only source: "btcusdt@depth20@100ms" -> BTCUSDT.
        // It becomes the Kafka key, which is how the consumer recovers the symbol and how
        // depth ordering stays per-symbol.
        String symbol = symbolFromStream(rawStream);
        if (symbol == null) {
            log.warn("Cannot derive symbol from stream '{}'; dropping depth frame", rawStream);
            return;
        }

        depthParsed.increment();
        if (log.isTraceEnabled()) {
            log.trace("depth RX {} lastUpdateId={} bids={} asks={}",
                    symbol, depth.lastUpdateId(), depth.bids().size(), depth.asks().size());
        }

        try {
            depthTemplate.send(depthTopic, symbol, depth)
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            depthFailed.increment();
                            log.error("Kafka publish FAILED depth {} (lastUpdateId={}) -> {}",
                                    symbol, depth.lastUpdateId(), describe(error));
                        }
                    });
        } catch (RuntimeException ex) {
            depthFailed.increment();
            log.error("Kafka publish REJECTED depth {} -> {}", symbol, describe(ex));
        }
    }

    /** {@code "btcusdt@depth20@100ms"} -> {@code "BTCUSDT"}; null if unparseable. */
    private static String symbolFromStream(String stream) {
        int at = stream.indexOf('@');
        if (at <= 0) {
            return null;
        }
        return stream.substring(0, at).toUpperCase(Locale.ROOT);
    }

    private static String abbreviate(String value) {
        return value.length() <= LOG_FRAME_PREVIEW_CHARS
                ? value
                : value.substring(0, LOG_FRAME_PREVIEW_CHARS) + "...";
    }

    private static String describe(Throwable error) {
        if (error == null) {
            return "unknown";
        }
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = root.getMessage();
        return message == null ? root.getClass().getSimpleName()
                : root.getClass().getSimpleName() + ": " + message;
    }

    /** Internal marker: the socket ended without an error we could attribute. */
    static final class SessionClosedException extends RuntimeException {
        SessionClosedException(String message) {
            super(message);
        }
    }
}
