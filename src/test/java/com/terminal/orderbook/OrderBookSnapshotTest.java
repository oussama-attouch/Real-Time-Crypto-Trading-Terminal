package com.terminal.orderbook;

import com.terminal.ingestion.DepthEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Mapping tests for real Binance depth. No broker needed: this pins the two properties
 * that decide whether the ladder looks correct — exact cumulative totals and
 * best-price-first ordering — plus the spread, which is the thing that was visibly wrong
 * with the synthetic book.
 *
 * <p>Payload shape here is copied from a live frame:
 * {@code {"lastUpdateId":103614277455,"bids":[["59936.31000000","2.47881000"],...],"asks":[...]}}
 */
class OrderBookSnapshotTest {

    private static final String SYMBOL = "BTCUSDT";
    private static final long NOW = 1_791_000_000_000L;

    @Test
    @DisplayName("maps Binance arrays to levels with cumulative totals, best price first")
    void mapsArraysWithCumulativeTotals() {
        DepthEvent depth = new DepthEvent(10L,
                levels("59936.31000000", "1.0", "59934.49000000", "2.0", "59933.00000000", "3.0"),
                levels("59936.32000000", "0.5", "59936.40000000", "1.5"));

        OrderBookSnapshot snapshot = OrderBookSnapshot.from(SYMBOL, depth, 20, NOW);

        assertThat(snapshot.bids()).hasSize(3);
        assertThat(snapshot.bids().get(0).price()).isEqualTo("59936.31000000");
        assertThat(snapshot.bids().get(0).total()).isEqualTo("1.0");    // 1
        assertThat(snapshot.bids().get(1).total()).isEqualTo("3.0");    // 1 + 2
        assertThat(snapshot.bids().get(2).total()).isEqualTo("6.0");    // 1 + 2 + 3

        assertThat(snapshot.asks()).hasSize(2);
        assertThat(snapshot.asks().get(0).price()).isEqualTo("59936.32000000");
        assertThat(snapshot.asks().get(0).total()).isEqualTo("0.5");
        assertThat(snapshot.asks().get(1).total()).isEqualTo("2.0");    // 0.5 + 1.5
    }

    @Test
    @DisplayName("spread is exact and positive for a sane book")
    void spreadIsExact() {
        DepthEvent depth = new DepthEvent(11L,
                levels("59936.31000000", "2.47881000"),
                levels("59936.32000000", "0.39034000"));

        OrderBookSnapshot snapshot = OrderBookSnapshot.from(SYMBOL, depth, 20, NOW);

        assertThat(snapshot.bestBid()).isEqualTo("59936.31000000");
        assertThat(snapshot.bestAsk()).isEqualTo("59936.32000000");
        // Exact decimal subtraction: no 0.010000000000005116 from a double.
        assertThat(new BigDecimal(snapshot.spread())).isEqualByComparingTo(new BigDecimal("0.01"));
        assertThat(new BigDecimal(snapshot.spread()).signum()).isPositive();
    }

    @Test
    @DisplayName("sides are re-sorted defensively even if the feed sends them out of order")
    void sortsDefensively() {
        DepthEvent depth = new DepthEvent(12L,
                // scrambled bids
                levels("59933.00000000", "1", "59936.31000000", "1", "59934.49000000", "1"),
                // scrambled asks
                levels("59936.40000000", "1", "59936.32000000", "1", "59936.36000000", "1"));

        OrderBookSnapshot snapshot = OrderBookSnapshot.from(SYMBOL, depth, 20, NOW);

        assertThat(prices(snapshot.bids()))
                .containsExactly("59936.31000000", "59934.49000000", "59933.00000000");   // descending
        assertThat(prices(snapshot.asks()))
                .containsExactly("59936.32000000", "59936.36000000", "59936.40000000");   // ascending
        assertThat(new BigDecimal(snapshot.spread()).signum()).isPositive();
    }

    @Test
    @DisplayName("depth limit truncates from the best price and totals stay consistent")
    void honoursDepthLimit() {
        DepthEvent depth = new DepthEvent(13L,
                levels("100.00", "1", "99.00", "2", "98.00", "3"),
                levels());

        OrderBookSnapshot snapshot = OrderBookSnapshot.from(SYMBOL, depth, 2, NOW);

        assertThat(snapshot.bids()).hasSize(2);
        assertThat(prices(snapshot.bids())).containsExactly("100.00", "99.00");
        assertThat(snapshot.asks()).isEmpty();
        assertThat(snapshot.bestAsk()).isEmpty();
        assertThat(snapshot.spread()).isEmpty();   // one-sided book
    }

    @Test
    @DisplayName("malformed rows are skipped individually, not fatal to the snapshot")
    void skipsMalformedRows() {
        List<List<String>> bids = new ArrayList<>();
        bids.add(Arrays.asList("100.00", "1"));
        bids.add(Arrays.asList("99.00"));                    // wrong arity
        bids.add(Arrays.asList("nonsense", "2"));            // unparseable price
        bids.add(Arrays.asList(null, "2"));                  // null price
        bids.add(Arrays.asList("98.00", "3"));

        OrderBookSnapshot snapshot = OrderBookSnapshot.from(
                SYMBOL, new DepthEvent(14L, bids, levels("101.00", "1")), 20, NOW);

        assertThat(prices(snapshot.bids())).containsExactly("100.00", "98.00");
        assertThat(snapshot.bids().get(1).total()).isEqualTo("4");   // 1 + 3, skipping bad rows
    }

    @Test
    @DisplayName("empty depth yields an empty but well-formed snapshot")
    void handlesEmptyDepth() {
        OrderBookSnapshot snapshot = OrderBookSnapshot.from(SYMBOL, new DepthEvent(15L, List.of(), List.of()), 20, NOW);

        assertThat(snapshot.bids()).isEmpty();
        assertThat(snapshot.asks()).isEmpty();
        assertThat(snapshot.bestBid()).isEmpty();
        assertThat(snapshot.bestAsk()).isEmpty();
        assertThat(snapshot.spread()).isEmpty();
        assertThat(snapshot.symbol()).isEqualTo(SYMBOL);
    }

    // ---------------------------------------------------------------------

    private static List<String> prices(List<OrderBookSnapshot.Level> levels) {
        return levels.stream().map(OrderBookSnapshot.Level::price).toList();
    }

    /** Builds [["price","qty"], ...] pairs from a flat varargs list. */
    private static List<List<String>> levels(String... priceQtyPairs) {
        List<List<String>> rows = new ArrayList<>(priceQtyPairs.length / 2);
        for (int i = 0; i + 1 < priceQtyPairs.length; i += 2) {
            rows.add(Arrays.asList(priceQtyPairs[i], priceQtyPairs[i + 1]));
        }
        return rows;
    }
}
