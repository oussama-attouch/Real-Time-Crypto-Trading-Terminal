package com.terminal.aggregation;

import com.terminal.ingestion.TradeEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Builds 1-minute OHLCV candles from the raw trade stream.
 *
 * <h2>Windowing</h2>
 * <p>The window comes from the trade's own timestamp, not from arrival time:
 * {@code openTime = tradeTime - (tradeTime % 60_000)}. That makes the aggregation a
 * function of the data rather than of when we happened to process it, so a trade that
 * spent extra time in the pipeline still lands in the minute it belongs to.
 *
 * <h2>Thread safety</h2>
 * <p>Kafka orders within a partition and trades are keyed by symbol, so all BTCUSDT
 * trades reach one listener thread — the per-symbol state is effectively single-writer.
 * That is a property of the topology, not something to lean on silently: a rebalance can
 * move a partition to another thread, and tests call in concurrently. So the state lives
 * in a {@link ConcurrentHashMap} and every mutation happens inside
 * {@code compute(key, ...)}, which is serialised per key. The accumulator is then a plain
 * mutable object, safe because it is only touched inside that critical section — one lock
 * per symbol rather than an atomic per field, which would be both slower and harder to
 * keep consistent.
 *
 * <h2>Late trades</h2>
 * <p>A trade older than the current window is <b>never</b> folded into the live candle —
 * that would corrupt the current minute. It is offered to the candle we just closed,
 * which we retain for {@code terminal.aggregation.late-tolerance}: if it belongs there
 * and we closed it recently, the trade is merged and the corrected candle is republished;
 * otherwise it is dropped and counted. Republishing is the honest option — the
 * alternative is a candle that is quietly wrong — and it is what every exchange kline
 * feed does, so consumers upsert by {@code closeTime}.
 */
@Slf4j
@Component
public class TradeAggregator {

    private static final long MINUTE_MS = 60_000L;

    private final KafkaTemplate<String, CandleEvent> candleTemplate;
    private final SimpMessagingTemplate messagingTemplate;
    private final Clock clock;
    private final String candleTopic;
    private final String liveDestination;
    private final long lateToleranceMillis;

    private final ConcurrentHashMap<String, SymbolState> states = new ConcurrentHashMap<>();

    private final Counter candlesPublished;
    private final Counter lateCorrected;
    private final Counter lateDropped;

    public TradeAggregator(KafkaTemplate<String, CandleEvent> candleTemplate,
                           SimpMessagingTemplate messagingTemplate,
                           Clock clock,
                           @Value("${terminal.kafka.topics.klines-1m:binance-klines-1m}")
                           String candleTopic,
                           @Value("${terminal.delivery.websocket.live-candle-topic:/topic/klines/live}")
                           String liveDestination,
                           @Value("${terminal.aggregation.late-tolerance:5s}") Duration lateTolerance,
                           MeterRegistry meterRegistry) {
        this.candleTemplate = candleTemplate;
        this.messagingTemplate = messagingTemplate;
        this.clock = clock;
        this.candleTopic = candleTopic;
        this.liveDestination = liveDestination;
        this.lateToleranceMillis = lateTolerance.toMillis();
        this.candlesPublished = Counter.builder("terminal.aggregation.candles.published")
                .description("Finalized candles published to Kafka").register(meterRegistry);
        this.lateCorrected = Counter.builder("terminal.aggregation.late.corrected")
                .description("Late trades merged into an already-closed candle").register(meterRegistry);
        this.lateDropped = Counter.builder("terminal.aggregation.late.dropped")
                .description("Trades too late to be attributed to any retained candle").register(meterRegistry);
    }

    /**
     * Consumes raw trades in its <b>own consumer group</b>. Sharing
     * {@code terminal-delivery-group} would split the partitions between this listener and
     * {@code TradeKafkaConsumer}, and each would see roughly half the trades — every candle
     * would then be built from half the market.
     */
    @KafkaListener(
            id = "trade-aggregator",
            topics = "${terminal.kafka.topics.raw-trades:binance-raw-trades}",
            groupId = "terminal-aggregator-group")
    public void onTrade(TradeEvent trade) {
        if (trade == null || trade.symbol() == null || trade.price() == null || trade.quantity() == null) {
            log.warn("Ignoring incomplete trade for aggregation: {}", trade);
            return;
        }

        long openTime = trade.tradeTime() - (trade.tradeTime() % MINUTE_MS);
        long now = clock.millis();

        AtomicReference<CandleEvent> toPublish = new AtomicReference<>();
        AtomicReference<CandleAccumulator> liveNow = new AtomicReference<>();

        states.compute(trade.symbol(), (symbol, existing) -> {
            SymbolState state = existing != null ? existing : new SymbolState();

            if (state.current == null) {
                state.current = CandleAccumulator.first(trade, openTime);
            } else if (openTime > state.current.openTime) {
                // Rollover: publish the finished candle now, but keep it as `previous`
                // so bounded late arrivals can still correct it.
                toPublish.set(state.current.toEvent());
                state.previous = state.current;
                state.previousClosedAt = now;
                state.current = CandleAccumulator.first(trade, openTime);
            } else if (openTime == state.current.openTime) {
                state.current.add(trade);
            } else if (state.previous != null
                    && openTime == state.previous.openTime
                    && (now - state.previousClosedAt) <= lateToleranceMillis) {
                state.previous.add(trade);
                lateCorrected.increment();
                toPublish.set(state.previous.toEvent());     // corrected republish
            } else {
                lateDropped.increment();
                log.debug("Dropping trade {} at {} — window {} closed and beyond tolerance",
                        trade.tradeId(), trade.tradeTime(), openTime);
                return state;
            }

            liveNow.set(state.current);
            return state;
        });

        CandleEvent closed = toPublish.get();
        if (closed != null) {
            publish(closed);
        }
        CandleAccumulator live = liveNow.get();
        if (live != null) {
            broadcastLive(live.toEvent());
        }
    }

    /**
     * Published with the symbol as key, so candle order per symbol is preserved exactly
     * as trade order is.
     */
    private void publish(CandleEvent candle) {
        candleTemplate.send(candleTopic, candle.symbol(), candle)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        log.error("Candle publish failed for {} @{}: {}",
                                candle.symbol(), candle.openTime(), error.toString());
                        return;
                    }
                    candlesPublished.increment();
                    if (log.isDebugEnabled()) {
                        var meta = result == null ? null : result.getRecordMetadata();
                        log.debug("Candle closed {} O={} H={} L={} C={} V={} n={} -> {}{}",
                                candle.symbol(), candle.open(), candle.high(), candle.low(),
                                candle.close(), candle.volume(), candle.tradeCount(), candleTopic,
                                meta == null ? "" : " p" + meta.partition() + "@" + meta.offset());
                    }
                });
    }

    /** Tick-by-tick update of the in-progress candle so the chart moves continuously. */
    private void broadcastLive(CandleEvent live) {
        try {
            messagingTemplate.convertAndSend(liveDestination, live);
        } catch (Exception ex) {
            // A UI fan-out failure must never disturb aggregation.
            log.error("Live candle broadcast failed for {}: {}", live.symbol(), ex.toString());
        }
    }

    /** Number of symbols currently holding an open candle (diagnostics/tests). */
    public int trackedSymbols() {
        return states.size();
    }

    // ---------------------------------------------------------------------
    // State
    // ---------------------------------------------------------------------

    /** Mutable holder for one symbol; mutated only inside {@code states.compute}. */
    private static final class SymbolState {
        CandleAccumulator current;
        CandleAccumulator previous;
        long previousClosedAt;
    }

    /**
     * Accumulates one window.
     *
     * <h3>String vs BigDecimal, field by field</h3>
     * <ul>
     *   <li>{@code open}/{@code close} are copied verbatim — no arithmetic, so the exact
     *       exchange string (trailing zeros included) survives to the chart.</li>
     *   <li>{@code high}/{@code low} must be <b>compared</b>, and lexical comparison is
     *       wrong ({@code "99.5" > "100.0"}), so each kept value carries a
     *       {@link BigDecimal} for the comparison while the emitted string stays the
     *       original from the winning trade.</li>
     *   <li>{@code volume} must be <b>summed</b>, which is the one place a float visibly
     *       lies ({@code 0.1 + 0.2 = 0.30000000000000004}). BigDecimal is exact and is
     *       turned into a plain decimal string only at publication.</li>
     * </ul>
     */
    private static final class CandleAccumulator {

        private final String symbol;
        private final long openTime;
        private final String open;

        private String high;
        private BigDecimal highValue;
        private String low;
        private BigDecimal lowValue;

        private String close;
        private BigDecimal volume;
        private long tradeCount;

        private CandleAccumulator(TradeEvent trade, long openTime) {
            this.symbol = trade.symbol();
            this.openTime = openTime;
            this.open = trade.price();
            this.high = trade.price();
            this.highValue = new BigDecimal(trade.price());
            this.low = trade.price();
            this.lowValue = this.highValue;
            this.close = trade.price();
            this.volume = new BigDecimal(trade.quantity());
            this.tradeCount = 1;
        }

        static CandleAccumulator first(TradeEvent trade, long openTime) {
            return new CandleAccumulator(trade, openTime);
        }

        void add(TradeEvent trade) {
            BigDecimal price = new BigDecimal(trade.price());
            if (price.compareTo(highValue) > 0) {
                high = trade.price();
                highValue = price;
            }
            if (price.compareTo(lowValue) < 0) {
                low = trade.price();
                lowValue = price;
            }
            close = trade.price();
            volume = volume.add(new BigDecimal(trade.quantity()));
            tradeCount++;
        }

        CandleEvent toEvent() {
            return new CandleEvent(
                    symbol,
                    openTime,
                    openTime + MINUTE_MS - 1,
                    open,
                    high,
                    low,
                    close,
                    // toPlainString: never 1.2E+4, and the exact scale is preserved.
                    volume.toPlainString(),
                    tradeCount);
        }
    }
}
