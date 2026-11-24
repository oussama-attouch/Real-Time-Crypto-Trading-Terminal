package com.terminal.orderbook;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.terminal.ingestion.DepthEvent;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * A point-in-time view of one symbol's book, ready to serialize.
 *
 * <p>Every number is a decimal {@code String}. The cumulative {@code total} is computed
 * here in {@link BigDecimal} so the browser never accumulates decimals — no float ever
 * touches a price. With real depth the totals are the actual resting size at each level,
 * which is what makes the depth bars meaningful rather than decorative.
 *
 * <p>{@code bestBid} / {@code bestAsk} / {@code spread} are empty strings when that side
 * has no levels, never {@code null}: {@code default-property-inclusion: non_null} would
 * otherwise drop the keys and the TypeScript type would be lying.
 *
 * @param bids best (highest) price first, as sent by Binance and re-sorted defensively
 * @param asks best (lowest) price first
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderBookSnapshot(

        @JsonProperty("symbol") String symbol,

        @JsonProperty("bids") List<Level> bids,

        @JsonProperty("asks") List<Level> asks,

        @JsonProperty("bestBid") String bestBid,

        @JsonProperty("bestAsk") String bestAsk,

        /** bestAsk - bestBid, exact decimal. */
        @JsonProperty("spread") String spread,

        @JsonProperty("timestamp") long timestamp) {

    /**
     * @param quantity resting size at this price (real, from the exchange)
     * @param total    cumulative quantity from the best price up to and including this
     *                 level — exactly the number the depth bar width is derived from
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Level(
            @JsonProperty("price") String price,
            @JsonProperty("quantity") String quantity,
            @JsonProperty("total") String total) {
    }

    public static OrderBookSnapshot empty(String symbol, long timestamp) {
        return new OrderBookSnapshot(symbol, List.of(), List.of(), "", "", "", timestamp);
    }

    /**
     * Maps a raw Binance depth frame onto a serializable snapshot.
     *
     * <p>Binance already sends bids descending and asks ascending, but both sides are
     * re-sorted here rather than trusted: it costs ~20 comparisons, and it makes the
     * "best price first" invariant a property of this method instead of an assumption
     * about an external feed. Malformed rows (wrong arity, unparseable number) are skipped
     * individually — one bad level must not discard the whole snapshot.
     */
    public static OrderBookSnapshot from(String symbol, DepthEvent depth, int maxLevels, long timestamp) {
        List<Level> bids = levels(depth.bids(), maxLevels, true);    // highest first
        List<Level> asks = levels(depth.asks(), maxLevels, false);   // lowest first

        Level bestBid = bids.isEmpty() ? null : bids.get(0);
        Level bestAsk = asks.isEmpty() ? null : asks.get(0);

        String bestBidText = bestBid == null ? "" : bestBid.price();
        String bestAskText = bestAsk == null ? "" : bestAsk.price();

        String spreadText = bestBid == null || bestAsk == null
                ? ""
                : new BigDecimal(bestAsk.price()).subtract(new BigDecimal(bestBid.price())).toPlainString();

        return new OrderBookSnapshot(symbol, bids, asks, bestBidText, bestAskText, spreadText, timestamp);
    }

    /** A parsed level before the running total is attached. */
    private record PricedLevel(BigDecimal price, BigDecimal quantity) {
    }

    private static List<Level> levels(List<List<String>> raw, int maxLevels, boolean descending) {
        if (raw == null || raw.isEmpty()) {
            return List.of();
        }

        List<PricedLevel> parsed = new ArrayList<>(raw.size());
        for (List<String> row : raw) {
            if (row == null || row.size() < 2) {
                continue;
            }
            try {
                parsed.add(new PricedLevel(new BigDecimal(row.get(0)), new BigDecimal(row.get(1))));
            } catch (NumberFormatException | NullPointerException ex) {
                // Skip just this level; the rest of the book is still valid.
            }
        }

        parsed.sort(descending
                ? Comparator.comparing(PricedLevel::price).reversed()
                : Comparator.comparing(PricedLevel::price));

        int limit = Math.min(Math.max(0, maxLevels), parsed.size());
        List<Level> out = new ArrayList<>(limit);
        BigDecimal cumulative = BigDecimal.ZERO;
        for (int i = 0; i < limit; i++) {
            PricedLevel level = parsed.get(i);
            cumulative = cumulative.add(level.quantity());
            out.add(new Level(
                    level.price().toPlainString(),
                    level.quantity().toPlainString(),
                    // toPlainString: never 1.2E+4, exact scale preserved.
                    cumulative.toPlainString()));
        }
        return out;
    }
}
