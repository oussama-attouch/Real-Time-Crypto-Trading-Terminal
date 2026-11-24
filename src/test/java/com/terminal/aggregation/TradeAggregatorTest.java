package com.terminal.aggregation;

import com.terminal.ingestion.TradeEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for the candle windowing logic.
 *
 * <p>These need no broker, no Docker and no Spring context: the aggregator is a plain
 * object whose collaborators are mocked. That matters here — the aggregation rules
 * (window boundaries, OHLCV updates, rollover, late-trade policy) are the part most
 * likely to be silently wrong, and they can be pinned down precisely without a running
 * cluster.
 */
@ExtendWith(MockitoExtension.class)
class TradeAggregatorTest {

    private static final String SYMBOL = "BTCUSDT";
    private static final String CANDLE_TOPIC = "binance-klines-1m";
    private static final String LIVE_DESTINATION = "/topic/klines/live";

    /** 2026-11-24T15:00:00Z — an exact minute boundary. */
    private static final long MINUTE = 1_791_000_000_000L / 60_000L * 60_000L;
    private static final long MINUTE_2 = MINUTE + 60_000L;

    @Mock
    private KafkaTemplate<String, CandleEvent> candleTemplate;

    @Mock
    private SimpMessagingTemplate messagingTemplate;

    private TradeAggregator aggregator;

    @BeforeEach
    void setUp() {
        // send() returns a future; the mock must not hand back null where the aggregator
        // calls whenComplete. lenient() because not every test publishes a candle.
        lenient().when(candleTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.<SendResult<String, CandleEvent>>completedFuture(null));

        // Fixed clock placed after every trade in these tests, so the late-tolerance
        // window (now - previousClosedAt) is measured from the rollover instant rather
        // than from wall-clock drift.
        Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(MINUTE_2 + 30_000), ZoneOffset.UTC);

        aggregator = new TradeAggregator(
                candleTemplate,
                messagingTemplate,
                fixedClock,
                CANDLE_TOPIC,
                LIVE_DESTINATION,
                Duration.ofSeconds(5),
                new SimpleMeterRegistry());
    }

    @Test
    @DisplayName("first trade of a minute opens a candle with O=H=L=C and broadcasts it live")
    void shouldCreateNewCandleOnFirstTrade() {
        aggregator.onTrade(trade(MINUTE + 5_000, "100.50", "1.5"));

        // Nothing is finalized yet: the minute is still open.
        verify(candleTemplate, never()).send(anyString(), anyString(), any());

        CandleEvent live = lastLive();
        assertThat(live.symbol()).isEqualTo(SYMBOL);
        assertThat(live.openTime()).isEqualTo(MINUTE);
        assertThat(live.closeTime()).isEqualTo(MINUTE + 59_999);
        assertThat(live.open()).isEqualTo("100.50");
        assertThat(live.high()).isEqualTo("100.50");
        assertThat(live.low()).isEqualTo("100.50");
        assertThat(live.close()).isEqualTo("100.50");
        assertThat(live.volume()).isEqualTo("1.5");
        assertThat(live.tradeCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("subsequent same-minute trades update H/L/C and accumulate volume and count")
    void shouldUpdateHighLowCloseOnSameMinuteTrades() {
        aggregator.onTrade(trade(MINUTE + 1_000, "100.50", "1.5"));
        aggregator.onTrade(trade(MINUTE + 2_000, "99.25", "2.0"));    // new low
        aggregator.onTrade(trade(MINUTE + 3_000, "101.75", "0.25"));  // new high, new close

        CandleEvent live = lastLive();
        assertThat(live.open()).as("open is the first trade, not the lowest").isEqualTo("100.50");
        assertThat(live.high()).isEqualTo("101.75");
        assertThat(live.low()).isEqualTo("99.25");
        assertThat(live.close()).isEqualTo("101.75");
        assertThat(live.volume()).as("exact decimal sum, no float drift").isEqualTo("3.75");
        assertThat(live.tradeCount()).isEqualTo(3);
        assertThat(live.openTime()).isEqualTo(MINUTE);

        verify(candleTemplate, never()).send(anyString(), anyString(), any());
    }

    @Test
    @DisplayName("crossing a minute boundary publishes the previous candle and starts a new one")
    void shouldRolloverToNextMinuteAndPublishPrevious() {
        aggregator.onTrade(trade(MINUTE + 1_000, "100.00", "1.0"));
        aggregator.onTrade(trade(MINUTE + 2_000, "103.00", "0.5"));   // high within minute 1
        aggregator.onTrade(trade(MINUTE + 3_000, "102.00", "0.5"));   // close within minute 1
        aggregator.onTrade(trade(MINUTE_2 + 1_000, "104.00", "2.0")); // rolls over

        ArgumentCaptor<CandleEvent> published = ArgumentCaptor.forClass(CandleEvent.class);
        verify(candleTemplate, times(1)).send(eq(CANDLE_TOPIC), eq(SYMBOL), published.capture());

        CandleEvent finalized = published.getValue();
        assertThat(finalized.openTime()).isEqualTo(MINUTE);
        assertThat(finalized.closeTime()).isEqualTo(MINUTE + 59_999);
        assertThat(finalized.open()).isEqualTo("100.00");
        assertThat(finalized.high()).isEqualTo("103.00");
        assertThat(finalized.low()).isEqualTo("100.00");
        assertThat(finalized.close()).isEqualTo("102.00");
        assertThat(finalized.volume()).isEqualTo("2.0");
        assertThat(finalized.tradeCount()).isEqualTo(3);

        // The new minute is open and independent.
        CandleEvent live = lastLive();
        assertThat(live.openTime()).isEqualTo(MINUTE_2);
        assertThat(live.open()).isEqualTo("104.00");
        assertThat(live.close()).isEqualTo("104.00");
        assertThat(live.volume()).isEqualTo("2.0");
        assertThat(live.tradeCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("a trade that belongs to the previous minute, inside tolerance, corrects and republishes that candle")
    void shouldMergeLateTradeWithinToleranceAndRepublish() {
        aggregator.onTrade(trade(MINUTE + 10_000, "100.00", "1.0"));
        aggregator.onTrade(trade(MINUTE_2 + 1_000, "101.00", "1.0")); // rollover -> publishes minute 1

        // Late trade: tradeTime is inside minute 1, but it arrives while minute 2 is open.
        aggregator.onTrade(trade(MINUTE + 30_000, "99.50", "0.5"));

        ArgumentCaptor<CandleEvent> published = ArgumentCaptor.forClass(CandleEvent.class);
        verify(candleTemplate, times(2)).send(eq(CANDLE_TOPIC), eq(SYMBOL), published.capture());

        CandleEvent first = published.getAllValues().get(0);
        CandleEvent corrected = published.getAllValues().get(1);

        assertThat(first.openTime()).isEqualTo(MINUTE);
        assertThat(first.volume()).isEqualTo("1.0");

        assertThat(corrected.openTime()).as("same window, corrected").isEqualTo(MINUTE);
        assertThat(corrected.low()).as("late trade set a new low").isEqualTo("99.50");
        assertThat(corrected.close()).as("late trade is the newest in that window").isEqualTo("99.50");
        assertThat(corrected.volume()).isEqualTo("1.5");
        assertThat(corrected.tradeCount()).isEqualTo(2);

        // The live candle must NOT have absorbed the late trade.
        assertThat(lastLive().openTime()).isEqualTo(MINUTE_2);
        assertThat(lastLive().volume()).isEqualTo("1.0");
    }

    // ---------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------

    private CandleEvent lastLive() {
        ArgumentCaptor<CandleEvent> captor = ArgumentCaptor.forClass(CandleEvent.class);
        verify(messagingTemplate, org.mockito.Mockito.atLeastOnce())
                .convertAndSend(eq(LIVE_DESTINATION), captor.capture());
        return captor.getAllValues().get(captor.getAllValues().size() - 1);
    }

    private static TradeEvent trade(long tradeTime, String price, String quantity) {
        return new TradeEvent(
                "999" + tradeTime,   // tradeId
                SYMBOL,
                price,
                quantity,
                tradeTime,
                false);
    }
}
