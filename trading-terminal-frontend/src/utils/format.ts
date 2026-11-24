/**
 * Display formatters for exchange-precision decimals.
 *
 * The wire format is the exchange's own: `"59825.98000000"`, `"0.00012000"`. That is the
 * right thing to store, log and submit — it is lossless — but in a 280px column it
 * overflows and gets ellipsised into `59825.980000…`, which is worse than useless in a
 * ladder or a tape.
 *
 * These helpers are **presentation only**. `Number()` is used to round for display; the
 * exact strings remain the source of truth everywhere else in the app.
 */

/** Prices to the exchange's tick granularity for BTCUSDT (2 dp). */
export function formatPrice(raw: string): string {
  const value = Number(raw);
  return Number.isFinite(value) && value > 0 ? value.toFixed(2) : raw;
}

/**
 * Quantities to 4 dp, with one deliberate exception: if rounding to 4 dp would render real
 * liquidity as `0.0000`, the trimmed original is shown instead. Displaying zero size for a
 * level or a print that exists is worse than showing an extra digit.
 */
export function formatQty(raw: string): string {
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    return raw;
  }
  const fixed = value.toFixed(4);
  return Number(fixed) === 0 ? raw.replace(/0+$/, '').replace(/\.$/, '') : fixed;
}
