import { useEffect, useRef } from 'react';
import {
  CandlestickSeries,
  ColorType,
  CrosshairMode,
  HistogramSeries,
  createChart,
  type CandlestickData,
  type HistogramData,
  type UTCTimestamp,
} from 'lightweight-charts';
import { useTradingStore } from '../store/tradingStore';
import type { CandleEvent } from '../types/trade';
import { generateMockCandles } from '../utils/mockHistory';

const COLOR_UP = '#0ecb81';
const COLOR_DOWN = '#f6465d';
const COLOR_BG = '#0b0e11';
const COLOR_GRID = '#1e2329';
const COLOR_BORDER = '#2b3139';
const COLOR_TEXT = '#848e9c';
const COLOR_VOLUME_UP = 'rgba(14, 203, 129, 0.5)';
const COLOR_VOLUME_DOWN = 'rgba(246, 70, 93, 0.5)';

/** Placeholder bars drawn when the store has no history yet. See `mockHistory.ts`. */
const MOCK_BAR_COUNT = 100;
const FALLBACK_PRICE = 60_000;

/**
 * Lightweight Charts wants SECONDS since epoch, not millis — `Math.floor(openTime / 1000)`
 * is mandatory and feeding it millis throws rather than drawing something wrong.
 *
 * The other conversion is `String -> number`, and this is the only place in the app where
 * a price becomes a float: the chart is a canvas renderer and needs numerics. Nothing
 * computed here flows back into state or into an order.
 */
function toBar(candle: CandleEvent): CandlestickData<UTCTimestamp> | null {
  const open = Number(candle.open);
  const high = Number(candle.high);
  const low = Number(candle.low);
  const close = Number(candle.close);
  const time = Math.floor(candle.openTime / 1000);

  // A non-finite or zero price means a malformed bar. One bar with low=0 draws a wick from
  // 0 to 60k — a full-height canvas line — so it is dropped rather than rendered.
  if (
    !Number.isFinite(time) ||
    time <= 0 ||
    !Number.isFinite(open) ||
    !Number.isFinite(high) ||
    !Number.isFinite(low) ||
    !Number.isFinite(close) ||
    open <= 0 ||
    high <= 0 ||
    low <= 0 ||
    close <= 0
  ) {
    return null;
  }

  // A partial candle can briefly report high < low after a late trade; clamp so the
  // renderer never sees an inverted range.
  return {
    time: time as UTCTimestamp,
    open,
    high: Math.max(high, low, open, close),
    low: Math.min(high, low, open, close),
    close,
  };
}

function toVolume(
  candle: CandleEvent,
  bar: CandlestickData<UTCTimestamp>,
): HistogramData<UTCTimestamp> {
  const volume = Number(candle.volume);
  return {
    time: bar.time,
    value: Number.isFinite(volume) && volume > 0 ? volume : 0,
    // Green when the bar closed up, red when it closed down.
    color: bar.close >= bar.open ? COLOR_VOLUME_UP : COLOR_VOLUME_DOWN,
  };
}

function sameBar(a: CandlestickData<UTCTimestamp>, b: CandlestickData<UTCTimestamp>): boolean {
  return a.open === b.open && a.high === b.high && a.low === b.low && a.close === b.close;
}

export function CandleChart(): JSX.Element {
  const containerRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const container = containerRef.current;
    if (container === null) {
      return;
    }

    const chart = createChart(container, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: COLOR_BG },
        textColor: COLOR_TEXT,
        fontSize: 11,
      },
      grid: {
        vertLines: { color: COLOR_GRID },
        horzLines: { color: COLOR_GRID },
      },
      rightPriceScale: { borderColor: COLOR_BORDER },
      timeScale: {
        borderColor: COLOR_BORDER,
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 4,
      },
      crosshair: { mode: CrosshairMode.Normal },
    });

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: COLOR_UP,
      downColor: COLOR_DOWN,
      borderVisible: false,
      wickUpColor: COLOR_UP,
      wickDownColor: COLOR_DOWN,
      priceFormat: { type: 'price', precision: 2, minMove: 0.01 },
    });

    /**
     * Volume overlays the price pane pinned to its bottom 20%: `priceScaleId: ''` makes it
     * an overlay rather than a second right-hand axis, and `scaleMargins` reserves the
     * space. Without both, the histogram is either invisible behind the price scale or
     * squashed against the axis.
     */
    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: 'volume' },
      priceScaleId: '',
      lastValueVisible: false,
      priceLineVisible: false,
    });
    volumeSeries.priceScale().applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    });

    // Shadow copies of everything on the canvas, keyed by bar time (seconds).
    const bars = new Map<number, CandlestickData<UTCTimestamp>>();
    const volumes = new Map<number, HistogramData<UTCTimestamp>>();
    let lastTime = 0;

    /** Full redraw from the shadow maps: first data, and late corrections. */
    const redraw = (fit: boolean) => {
      const orderedBars = Array.from(bars.values()).sort((a, b) => a.time - b.time);
      candleSeries.setData(orderedBars);
      volumeSeries.setData(
        orderedBars
          .map((bar) => volumes.get(bar.time))
          .filter((point): point is HistogramData<UTCTimestamp> => point !== undefined),
      );
      const newest = orderedBars[orderedBars.length - 1];
      if (newest !== undefined) {
        lastTime = newest.time;
      }
      // Fitting a single bar stretches it across the entire canvas — that is precisely
      // what a lone full-height "vertical line" looks like.
      if (fit && orderedBars.length > 1) {
        chart.timeScale().fitContent();
      }
    };

    const seed = (source: readonly CandleEvent[]) => {
      for (const candle of source) {
        const bar = toBar(candle);
        if (bar !== null) {
          bars.set(bar.time, bar);
          volumes.set(bar.time, toVolume(candle, bar));
        }
      }
    };

    // ---- initial load ----
    // Real history if the store has any; otherwise placeholder bars that END at the live
    // price, so the first real candle joins the series instead of jumping away from it.
    const realCandles = useTradingStore.getState().candles;
    if (realCandles.length > 0) {
      seed(realCandles);
      redraw(true);
    } else {
      const livePrice = Number(useTradingStore.getState().lastPrice);
      const anchor = Number.isFinite(livePrice) && livePrice > 0 ? livePrice : FALLBACK_PRICE;
      seed(generateMockCandles(MOCK_BAR_COUNT, anchor));
      redraw(true);
    }

    /**
     * Upsert one candle into both series.
     *
     * `update` replaces the bar with the same time or appends a newer one, and refuses an
     * older time. Older times happen for two reasons that both matter here: the backend
     * republishing a corrected candle behind the live bar, and the first real history
     * replacing placeholder bars. Both go through `redraw(false)`, which rebuilds in order
     * WITHOUT re-fitting the viewport — re-fitting is what would snap the chart under the
     * user's cursor as the mock series is swapped for real data.
     */
    const upsert = (candle: CandleEvent) => {
      const bar = toBar(candle);
      if (bar === null) {
        return;
      }
      const volume = toVolume(candle, bar);
      const existing = bars.get(bar.time);

      if (existing !== undefined && sameBar(existing, bar)) {
        return; // identical re-delivery; skipping avoids a repaint every flush
      }

      bars.set(bar.time, bar);
      volumes.set(bar.time, volume);

      if (bar.time < lastTime) {
        redraw(false); // late correction / replacing placeholder history in place
        return;
      }

      candleSeries.update(bar);
      volumeSeries.update(volume);
      if (bar.time > lastTime) {
        lastTime = bar.time;
      }
    };

    const unsubscribe = useTradingStore.subscribe((state, previous) => {
      // In-progress minute: one canvas update per 20 Hz store flush.
      if (state.liveCandle !== null && state.liveCandle !== previous.liveCandle) {
        upsert(state.liveCandle);
      }

      // Finalized candles: identity changes at most once a minute.
      if (state.candles !== previous.candles) {
        for (const candle of state.candles) {
          upsert(candle);
        }
      }
    });

    return () => {
      unsubscribe();
      chart.remove();
    };
  }, []);

  return <div className="chart" ref={containerRef} />;
}
