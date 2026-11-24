import type { CandleEvent } from '../types/trade';

const MINUTE_MS = 60_000;

/**
 * Deterministic PRNG (mulberry32).
 *
 * A fixed seed rather than `Math.random()` so the placeholder history looks identical on
 * every load — a chart that reshuffles its own past on each refresh is a genuinely
 * confusing thing to look at, and screenshots of a demo should be reproducible.
 */
function mulberry32(seed: number): () => number {
  let state = seed;
  return () => {
    state |= 0;
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296;
  };
}

/**
 * Generates a plausible 1-minute OHLCV random walk that **ends at `startingPrice`**.
 *
 * <h2>Why it walks backwards</h2>
 * The series exists to fill the left side of a chart whose live candle sits at the current
 * market price. Generating a walk forward from an arbitrary seed price would leave the
 * placeholder ending somewhere unrelated — and the first real candle would then appear as
 * a cliff. Instead the walk is generated in relative terms and then rescaled so its final
 * close equals `startingPrice`, which is the live price at mount. The seam is invisible.
 *
 * <h2>Where it must NOT go</h2>
 * These bars never enter the Zustand store. They exist only as the chart's initial
 * `setData()`, so nothing downstream (stats, order sizing, tests) can mistake synthetic
 * prices for market data. Real candles overwrite the bars they overlap and append after.
 *
 * @param count         number of 1-minute bars (≈100 fills a wide pane at default zoom)
 * @param startingPrice price the series must END at — pass the live price
 * @param seed          PRNG seed, fixed by default for reproducibility
 */
export function generateMockCandles(
  count: number,
  startingPrice: number,
  seed = 20261124,
): CandleEvent[] {
  if (!Number.isFinite(count) || count <= 0 || !Number.isFinite(startingPrice) || startingPrice <= 0) {
    return [];
  }

  const random = mulberry32(seed);

  // 1. relative walk
  const levels: number[] = [];
  let level = 1;
  for (let i = 0; i < count; i += 1) {
    const drift = (random() - 0.5) * 0.0022; // ±0.11% per minute: busy but not absurd
    level *= 1 + drift;
    levels.push(level);
  }

  // 2. rescale so the LAST close lands exactly on startingPrice
  const lastLevel = levels[levels.length - 1];
  if (lastLevel === undefined || lastLevel <= 0) {
    return [];
  }
  const scale = startingPrice / lastLevel;

  // 3. emit OHLCV. Every numeric field is a fixed-scale decimal STRING, matching the wire
  //    format produced by the aggregation service.
  const lastOpenTime = Math.floor(Date.now() / MINUTE_MS) * MINUTE_MS;
  const candles: CandleEvent[] = [];

  let previousClose = (levels[0] ?? 1) * scale;
  for (let i = 0; i < count; i += 1) {
    const levelClose = levels[i];
    if (levelClose === undefined) {
      continue;
    }

    const close = levelClose * scale;
    // First bar invents its own open; later bars open where the previous one closed, so
    // the series has no gaps (an exchange chart never shows a gap within one session).
    const open = i === 0 ? close * (1 + (random() - 0.5) * 0.0008) : previousClose;
    previousClose = close;

    const bodyHigh = Math.max(open, close);
    const bodyLow = Math.min(open, close);
    const wick = bodyHigh * (0.0002 + random() * 0.0006);
    const high = bodyHigh + wick;
    const low = Math.max(bodyLow - wick, 0.01);

    // Volume scales with the size of the move: quiet minutes trade less.
    const moveRatio = Math.abs(close - open) / open;
    const volume = (0.35 + random() * 3.0) * (1 + moveRatio * 180);

    const openTime = lastOpenTime - (count - 1 - i) * MINUTE_MS;

    candles.push({
      symbol: 'BTCUSDT',
      openTime,
      closeTime: openTime + MINUTE_MS - 1,
      open: open.toFixed(2),
      high: high.toFixed(2),
      low: low.toFixed(2),
      close: close.toFixed(2),
      volume: volume.toFixed(5),
      tradeCount: Math.round(20 + random() * 180),
    });
  }

  return candles;
}
