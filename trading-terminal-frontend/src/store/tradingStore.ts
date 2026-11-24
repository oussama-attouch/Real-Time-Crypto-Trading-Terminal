import { create } from 'zustand';
import type {
  CandleEvent,
  OrderBookSnapshot,
  OrderPlacedEvent,
  PriceDirection,
  TradeEvent,
} from '../types/trade';

const MAX_TRADES = 50;
const MAX_CANDLES = 500;
/** 50 ms => at most 20 store commits per second, whatever the socket is doing. */
const FLUSH_INTERVAL_MS = 50;
/** Guard against unbounded growth if the tab is backgrounded (intervals are throttled). */
const MAX_PENDING_TRADES = 2_000;

export interface TradingState {
  connected: boolean;
  /** Newest first, capped at 50. */
  trades: TradeEvent[];
  /** Ascending by openTime (what the chart needs), upserted — never appended. */
  candles: CandleEvent[];
  /** In-progress 1m candle; the chart keeps the last bar live from this. */
  liveCandle: CandleEvent | null;
  lastPrice: string;
  lastPriceChange: PriceDirection;
  /** Latest L2 snapshot, replaced wholesale (the backend already throttles to 10 Hz). */
  orderBook: OrderBookSnapshot | null;
  /**
   * Most recent order confirmation received over STOMP. Set directly, never through the
   * flush buffer: an order event is an individual fact, and coalescing two of them inside
   * one 50 ms window would silently drop a confirmation.
   */
  lastOrderEvent: OrderPlacedEvent | null;

  setConnected: (connected: boolean) => void;
  applySnapshot: (snapshot: TradeEvent[]) => void;
  ingestTrade: (trade: TradeEvent) => void;
  ingestLiveCandle: (candle: CandleEvent) => void;
  ingestClosedCandle: (candle: CandleEvent) => void;
  ingestOrderBook: (snapshot: OrderBookSnapshot) => void;
  ingestOrderEvent: (event: OrderPlacedEvent) => void;
  clearOrderEvent: () => void;
}

// ---------------------------------------------------------------------------
// Write buffers. Deliberately module scope, NOT React state: writing here costs a
// couple of array pushes and triggers nothing. This is what decouples socket
// throughput from render rate.
// ---------------------------------------------------------------------------
const pendingTrades: TradeEvent[] = [];
/** Keyed by openTime so a republished candle overwrites its predecessor in-flight. */
const pendingCandles = new Map<number, CandleEvent>();
let pendingLiveCandle: CandleEvent | null = null;
/** Only the newest book matters: a depth ladder is a state, not a stream of events. */
let pendingOrderBook: OrderBookSnapshot | null = null;
let droppedPendingTrades = 0;

/**
 * Merge two newest-first lists, de-duplicating by tradeId and ordering by tradeTime
 * descending. Ordering by time (rather than trusting concatenation) is what makes the
 * snapshot/live race safe: a live trade that arrived before the snapshot reply is newer
 * than everything in the snapshot and must stay on top.
 */
function mergeNewestFirst(a: readonly TradeEvent[], b: readonly TradeEvent[], cap: number): TradeEvent[] {
  const seen = new Set<string>();
  const merged: TradeEvent[] = [];
  for (const trade of [...a, ...b]) {
    if (seen.has(trade.tradeId)) {
      continue;
    }
    seen.add(trade.tradeId);
    merged.push(trade);
  }
  merged.sort((x, y) => y.tradeTime - x.tradeTime);
  return merged.length > cap ? merged.slice(0, cap) : merged;
}

function lastPriceOf(trades: readonly TradeEvent[]): string | null {
  let newest: TradeEvent | null = null;
  for (const trade of trades) {
    if (newest === null || trade.tradeTime > newest.tradeTime) {
      newest = trade;
    }
  }
  return newest === null ? null : newest.price;
}

/**
 * Direction is a display cue only. Comparing the decimal strings numerically needs a
 * number, and for an up/down arrow a double is plenty — no money is ever computed here.
 */
function direction(previous: string, next: string): PriceDirection {
  if (previous === '' || previous === next) {
    return 'flat';
  }
  return Number(next) > Number(previous) ? 'up' : 'down';
}

function upsertCandles(existing: readonly CandleEvent[], incoming: readonly CandleEvent[]): CandleEvent[] {
  const byOpenTime = new Map<number, CandleEvent>();
  for (const candle of existing) {
    byOpenTime.set(candle.openTime, candle);
  }
  for (const candle of incoming) {
    // A late trade re-publishes the same openTime with corrected OHLCV. Last write wins.
    byOpenTime.set(candle.openTime, candle);
  }
  const ordered = Array.from(byOpenTime.values()).sort((x, y) => x.openTime - y.openTime);
  return ordered.length > MAX_CANDLES ? ordered.slice(ordered.length - MAX_CANDLES) : ordered;
}

export const useTradingStore = create<TradingState>()((set, get) => ({
  connected: false,
  trades: [],
  candles: [],
  liveCandle: null,
  lastPrice: '',
  lastPriceChange: 'flat',
  orderBook: null,
  lastOrderEvent: null,

  setConnected: (connected) => {
    if (get().connected !== connected) {
      set({ connected });
    }
  },

  /** Called once per connection with the 50 most recent trades (newest first). */
  applySnapshot: (snapshot) => {
    set((state) => {
      const merged = mergeNewestFirst(snapshot, state.trades, MAX_TRADES);
      const price = lastPriceOf(merged);
      return {
        trades: merged,
        lastPrice: price ?? state.lastPrice,
        lastPriceChange: price === null ? state.lastPriceChange : direction(state.lastPrice, price),
      };
    });
  },

  // ---- buffers: no set() here. Cheap, allocation-light, called per message. ----
  ingestTrade: (trade) => {
    if (pendingTrades.length >= MAX_PENDING_TRADES) {
      pendingTrades.shift();
      droppedPendingTrades += 1;
    }
    pendingTrades.push(trade);
  },

  ingestLiveCandle: (candle) => {
    // Only the newest state of the open candle matters; overwrite freely.
    pendingLiveCandle = candle;
  },

  ingestClosedCandle: (candle) => {
    pendingCandles.set(candle.openTime, candle);
  },

  ingestOrderBook: (snapshot) => {
    // The backend already throttles to 10 Hz; buffering here keeps a single commit path
    // and collapses any burst (e.g. after a reconnect) into the latest state only.
    pendingOrderBook = snapshot;
  },

  // ---- order confirmations: committed immediately, deliberately outside the flush loop ----
  ingestOrderEvent: (event) => {
    set({ lastOrderEvent: event });
  },

  clearOrderEvent: () => {
    if (get().lastOrderEvent !== null) {
      set({ lastOrderEvent: null });
    }
  },
}));

/**
 * The single commit point. Runs 20x/s regardless of message rate; if nothing arrived it
 * returns before touching state, so an idle socket causes zero re-renders.
 */
function flush(): void {
  const hasTrades = pendingTrades.length > 0;
  const hasCandles = pendingCandles.size > 0;
  const live = pendingLiveCandle;
  const book = pendingOrderBook;

  if (!hasTrades && !hasCandles && live === null && book === null) {
    return;
  }

  // Detach the buffers first so anything arriving during the commit goes to the next tick.
  const tradeBatch = hasTrades ? pendingTrades.splice(0, pendingTrades.length) : [];
  const candleBatch = hasCandles ? Array.from(pendingCandles.values()) : [];
  pendingCandles.clear();
  pendingLiveCandle = null;
  pendingOrderBook = null;

  if (droppedPendingTrades > 0) {
    // eslint-disable-next-line no-console
    console.warn(`[tradingStore] dropped ${droppedPendingTrades} buffered trades (flush fell behind)`);
    droppedPendingTrades = 0;
  }

  useTradingStore.setState((state) => {
    const next: Partial<TradingState> = {};

    if (tradeBatch.length > 0) {
      // Live frames are chronological, so reversing yields newest-first with no sort.
      const newestFirst = tradeBatch.slice().reverse();
      next.trades = mergeNewestFirst(newestFirst, state.trades, MAX_TRADES);
      const price = lastPriceOf(tradeBatch);
      if (price !== null) {
        next.lastPrice = price;
        next.lastPriceChange = direction(state.lastPrice, price);
      }
    }

    if (candleBatch.length > 0) {
      next.candles = upsertCandles(state.candles, candleBatch);
    }

    if (live !== null) {
      next.liveCandle = live;
    }

    if (book !== null) {
      // Whole-object replacement: the ladder has no meaningful per-level diff to merge,
      // and a stale level left behind would render as phantom liquidity.
      next.orderBook = book;
    }

    return next;
  });
}

let flushTimer: ReturnType<typeof setInterval> | null = null;

/** Idempotent, and HMR-safe: re-importing the module must not stack intervals. */
export function startFlushLoop(): void {
  if (flushTimer !== null) {
    return;
  }
  flushTimer = setInterval(flush, FLUSH_INTERVAL_MS);
}

startFlushLoop();

if (import.meta.hot) {
  import.meta.hot.dispose(() => {
    if (flushTimer !== null) {
      clearInterval(flushTimer);
      flushTimer = null;
    }
  });
}
