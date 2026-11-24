/**
 * Wire contracts. These mirror the Java records exactly (field names, and the choice of
 * string for every decimal), so there is no adapter layer and no place for a silent
 * mismatch to hide.
 */

/** Mirrors {@code com.terminal.ingestion.TradeEvent}. */
export interface TradeEvent {
  /** 19-digit exchange id. STRING — parsing it as a number silently corrupts it. */
  tradeId: string;
  symbol: string;
  /** Exact decimal as sent by the exchange, e.g. "62921.04000000". Never a float. */
  price: string;
  /** Exact decimal, e.g. "0.00213000". */
  quantity: string;
  /** Epoch millis. */
  tradeTime: number;
  /** true => the buyer was the maker, i.e. the aggressor was the SELLER. */
  isBuyerMaker: boolean;
}

/** Mirrors {@code com.terminal.aggregation.CandleEvent}. */
export interface CandleEvent {
  symbol: string;
  /** Epoch millis, a multiple of 60_000. The chart key: convert to seconds for LWC. */
  openTime: number;
  /** Epoch millis, inclusive: openTime + 59_999. */
  closeTime: number;
  open: string;
  high: string;
  low: string;
  close: string;
  volume: string;
  tradeCount: number;
}

export type PriceDirection = 'up' | 'down' | 'flat';

/** STOMP destinations, kept in one place so a typo cannot silently subscribe to nothing. */
export const DESTINATIONS = {
  snapshot: '/app/trades',
  trades: '/topic/trades',
  liveCandle: '/topic/klines/live',
  closedCandles: '/topic/klines',
  orderBook: '/topic/orderbook',
  orders: '/topic/orders',
} as const;

/**
 * Confirmation that an order reached the read side: broadcast by the backend after the
 * event round-tripped Postgres -> Kafka -> `order-events`, not by the POST response.
 *
 * Mirrors {@code com.terminal.orders.OrderPlacedEvent}. `price` is an empty string (never
 * null) for a MARKET order, and `timestamp` is epoch millis.
 */
export interface OrderPlacedEvent {
  orderId: string;
  symbol: string;
  side: string;
  price: string;
  quantity: string;
  status: string;
  timestamp: number;
}

/** One price level of the L2 book, cumulative `total` already computed server-side. */
export interface OrderBookLevel {
  price: string;
  quantity: string;
  total: string;
}

/** Mirrors com.terminal.orderbook.OrderBookSnapshot. */
export interface OrderBookSnapshot {
  symbol: string;
  /** best (highest) price first */
  bids: OrderBookLevel[];
  /** best (lowest) price first */
  asks: OrderBookLevel[];
  /** empty string when that side is empty — never null, so the shape is stable */
  bestBid: string;
  bestAsk: string;
  spread: string;
  timestamp: number;
}
