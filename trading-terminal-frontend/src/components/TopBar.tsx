import { useMemo } from 'react';
import { useTradingStore } from '../store/tradingStore';
import type { CandleEvent } from '../types/trade';

/**
 * Symbol, live price, 24h change on the left; the stats strip in the middle; the
 * connection pill on the right.
 *
 * Selectors are separate primitives on purpose: an object selector allocates a new object
 * on every store commit and Zustand compares with `Object.is`, so this component would
 * re-render at 20 Hz even when nothing visible changed.
 *
 * The stats are folded over the candle window the client holds, and — critically — the
 * in-progress candle is folded in too. `/topic/klines/live` arrives from the moment the
 * first trade lands, while `/topic/klines` only delivers at a minute boundary, so
 * deriving only from closed candles leaves High/Low/Vol empty on a fresh page load.
 *
 * Cost: the 500-candle fold runs when `candles` changes (≤ once a minute); the live candle
 * is merged afterwards in O(1), so the 20 Hz price flush does not walk the array.
 */
interface Bounds {
  high: number;
  low: number;
  volume: number;
  notional: number;
  open: number;
}

function fold(bounds: Bounds | null, candle: CandleEvent): Bounds | null {
  const high = Number(candle.high);
  const low = Number(candle.low);
  if (!Number.isFinite(high) || !Number.isFinite(low) || high <= 0 || low <= 0) {
    return bounds; // malformed bar: ignore it rather than poisoning the range with NaN
  }

  const rawVolume = Number(candle.volume);
  const volume = Number.isFinite(rawVolume) && rawVolume > 0 ? rawVolume : 0;
  const notional = volume * ((high + low) / 2);

  if (bounds === null) {
    const open = Number(candle.open);
    return {
      high,
      low,
      volume,
      notional,
      open: Number.isFinite(open) && open > 0 ? open : high,
    };
  }

  return {
    high: Math.max(bounds.high, high),
    low: Math.min(bounds.low, low),
    volume: bounds.volume + volume,
    notional: bounds.notional + notional,
    open: bounds.open,
  };
}

/** 1_234_567 -> "1.23M". Keeps the stats strip from growing a scrollbar. */
function formatCompact(value: number): string {
  if (!Number.isFinite(value) || value <= 0) {
    return '—';
  }
  if (value >= 1_000_000_000) {
    return `${(value / 1_000_000_000).toFixed(2)}B`;
  }
  if (value >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(2)}M`;
  }
  if (value >= 1_000) {
    return `${(value / 1_000).toFixed(2)}K`;
  }
  return value.toFixed(2);
}

export function TopBar(): JSX.Element {
  const lastPrice = useTradingStore((state) => state.lastPrice);
  const lastPriceChange = useTradingStore((state) => state.lastPriceChange);
  const connected = useTradingStore((state) => state.connected);
  const candles = useTradingStore((state) => state.candles);
  const liveCandle = useTradingStore((state) => state.liveCandle);

  const closedBounds = useMemo(
    () => candles.reduce<Bounds | null>((acc, candle) => fold(acc, candle), null),
    [candles],
  );

  const bounds = useMemo(
    () => (liveCandle === null ? closedBounds : fold(closedBounds, liveCandle)),
    [closedBounds, liveCandle],
  );

  const changePercent = useMemo(() => {
    if (bounds === null || bounds.open <= 0) {
      return null;
    }
    const price = Number(lastPrice);
    if (!Number.isFinite(price) || price <= 0) {
      return null;
    }
    return ((price - bounds.open) / bounds.open) * 100;
  }, [bounds, lastPrice]);

  const changeDirection =
    changePercent === null
      ? 'flat'
      : changePercent > 0
        ? 'up'
        : changePercent < 0
          ? 'down'
          : 'flat';

  return (
    <header className="topbar">
      <div className="topbar__ident">
        <span className="topbar__symbol">
          BTCUSDT
          <span className="topbar__perp">PERP</span>
        </span>
        <span className="topbar__price-row">
          <span className={`topbar__price topbar__price--${lastPriceChange}`}>
            {lastPrice === '' ? '—' : lastPrice}
          </span>
          <span className={`topbar__change topbar__change--${changeDirection}`}>
            {changePercent === null
              ? '—'
              : `${changePercent > 0 ? '+' : ''}${changePercent.toFixed(2)}%`}
          </span>
        </span>
      </div>

      <div className="topbar__stats">
        <div className="stat">
          <span className="stat__label">24h High</span>
          <span className="stat__value">{bounds === null ? '—' : bounds.high.toFixed(2)}</span>
        </div>
        <div className="stat">
          <span className="stat__label">24h Low</span>
          <span className="stat__value">{bounds === null ? '—' : bounds.low.toFixed(2)}</span>
        </div>
        <div className="stat stat--optional">
          <span className="stat__label">24h Vol (BTC)</span>
          <span className="stat__value">{bounds === null ? '—' : bounds.volume.toFixed(3)}</span>
        </div>
        <div className="stat stat--optional">
          <span className="stat__label">24h Vol (USDT)</span>
          <span className="stat__value">{bounds === null ? '—' : formatCompact(bounds.notional)}</span>
        </div>
      </div>

      <div className="topbar__status">
        <span className={`status-pill ${connected ? 'status-pill--up' : 'status-pill--down'}`}>
          <span className="status-pill__dot" />
          {connected ? 'Connected' : 'Disconnected'}
        </span>
      </div>
    </header>
  );
}
