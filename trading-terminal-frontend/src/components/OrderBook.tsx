import { useEffect, useMemo, useRef, useState } from 'react';
import { FixedSizeList, type ListChildComponentProps } from 'react-window';
import { useTradingStore } from '../store/tradingStore';
import type { OrderBookLevel } from '../types/trade';
import { formatPrice, formatQty } from '../utils/format';

const ROW_HEIGHT = 18;
const SPREAD_HEIGHT = 24; // must match .orderbook__spread height in global.css

/** Re-exported so existing imports of these names keep working. */
export { formatPrice, formatQty };

interface BookRowData {
  levels: OrderBookLevel[];
  side: 'ask' | 'bid';
  /** Largest cumulative size across both visible sides, so the bars share one scale. */
  maxTotal: number;
}

/**
 * Module scope + itemData, so react-window keeps a stable component type.
 *
 * The depth bar is an absolutely positioned sibling *behind* the numbers, and the `key` on
 * the content node includes the level's quantity: when a level's size changes, React
 * unmounts and remounts that node, restarting the CSS flash from 0. No timers, no per-row
 * state, no list re-render.
 */
function BookRow({ index, style, data }: ListChildComponentProps<BookRowData>): JSX.Element {
  const level = data.levels[index];
  if (level === undefined) {
    return <div className="book-row" style={style} />;
  }

  // Number() is for the bar width only: a CSS percentage cannot be a decimal string.
  const pct = data.maxTotal > 0 ? (Number(level.total) / data.maxTotal) * 100 : 0;

  return (
    <div className="book-row" style={style}>
      <div
        className="book-row__bar"
        data-side={data.side}
        style={{ width: `${Math.min(100, pct)}%` }}
      />
      <div
        className="book-row__content"
        data-side={data.side}
        key={`${level.price}|${level.quantity}`}
      >
        <span className="book-row__price">{formatPrice(level.price)}</span>
        <span className="book-row__qty">{formatQty(level.quantity)}</span>
        <span className="book-row__total">{formatQty(level.total)}</span>
      </div>
    </div>
  );
}

export function OrderBook(): JSX.Element {
  const orderBook = useTradingStore((state) => state.orderBook);

  const ladderRef = useRef<HTMLDivElement | null>(null);
  const [size, setSize] = useState({ width: 0, height: 0 });

  useEffect(() => {
    const element = ladderRef.current;
    if (element === null) {
      return;
    }
    const observer = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (entry === undefined) {
        return;
      }
      setSize({
        width: Math.floor(entry.contentRect.width),
        height: Math.floor(entry.contentRect.height),
      });
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  const sideHeight = Math.max(0, Math.floor((size.height - SPREAD_HEIGHT) / 2));
  const rowsPerSide = Math.max(1, Math.floor(sideHeight / ROW_HEIGHT));

  /**
   * Defensive sorting. Binance already sends bids descending and asks ascending, and the
   * backend re-sorts them; sorting here too costs ~20 comparisons per 10 Hz frame and buys
   * an invariant — this component renders correctly whatever order arrives.
   *
   * Number() is for ORDERING only: prices are ~1e5 with 2 decimals, far inside double's
   * exact-integer range, and no arithmetic is performed on the result.
   */
  const sortedAsks = useMemo(
    () => [...(orderBook?.asks ?? [])].sort((a, b) => Number(a.price) - Number(b.price)),
    [orderBook],
  );
  const sortedBids = useMemo(
    () => [...(orderBook?.bids ?? [])].sort((a, b) => Number(b.price) - Number(a.price)),
    [orderBook],
  );

  // Asks arrive best-first (ascending). Take the levels nearest the spread, then reverse so
  // the best ask sits at the BOTTOM of the ask block, directly above the spread.
  const visibleAsks = sortedAsks.slice(0, rowsPerSide).reverse();
  const visibleBids = sortedBids.slice(0, rowsPerSide);

  // Cumulative totals grow outward from the spread, so the deepest visible level holds the
  // largest total on each side. One shared maximum keeps both ladders comparable.
  const deepestAsk = visibleAsks[visibleAsks.length - 1];
  const deepestBid = visibleBids[visibleBids.length - 1];
  const maxTotal = Math.max(
    deepestAsk === undefined ? 0 : Number(deepestAsk.total),
    deepestBid === undefined ? 0 : Number(deepestBid.total),
  );

  const hasData = orderBook !== null && (sortedBids.length > 0 || sortedAsks.length > 0);
  const spread = orderBook?.spread ?? '';

  return (
    <div className="orderbook">
      <div className="orderbook__head">
        <span>Price</span>
        <span className="align-right">Size</span>
        <span className="align-right">Total</span>
      </div>

      <div className="orderbook__ladder" ref={ladderRef}>
        {hasData && sideHeight > 0 ? (
          <>
            <div className="orderbook__side">
              <FixedSizeList<BookRowData>
                height={sideHeight}
                width={size.width}
                itemCount={visibleAsks.length}
                itemSize={ROW_HEIGHT}
                itemData={{ levels: visibleAsks, side: 'ask', maxTotal }}
                overscanCount={2}
              >
                {BookRow}
              </FixedSizeList>
            </div>

            <div className="orderbook__spread">
              <span className="orderbook__spread-label">Spread</span>
              <span className="orderbook__spread-value">
                {spread === '' ? '—' : formatPrice(spread)}
              </span>
            </div>

            <div className="orderbook__side">
              <FixedSizeList<BookRowData>
                height={sideHeight}
                width={size.width}
                itemCount={visibleBids.length}
                itemSize={ROW_HEIGHT}
                itemData={{ levels: visibleBids, side: 'bid', maxTotal }}
                overscanCount={2}
              >
                {BookRow}
              </FixedSizeList>
            </div>
          </>
        ) : (
          <div className="orderbook__empty">
            {orderBook === null ? 'waiting for depth…' : 'no liquidity in range'}
          </div>
        )}
      </div>
    </div>
  );
}
