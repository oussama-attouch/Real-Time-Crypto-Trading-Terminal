import { useState } from 'react';
import { CandleChart } from './components/CandleChart';
import { OrderBook } from './components/OrderBook';
import { OrderEntry } from './components/OrderEntry';
import { TopBar } from './components/TopBar';
import { TradeFeed } from './components/TradeFeed';

/**
 * Binance-futures-style shell:
 *
 *   ┌──────────────────────── header (64) ────────────────────────┐
 *   │ 65% chart           │ 15% book │ 20% ticket (60) + tape (40) │
 *   └──────────────────── positions bar (40) ─────────────────────┘
 *
 * The chart column is `minmax(0, 65fr)` and the middle row is `minmax(0, 1fr)`:
 * without an explicit 0 minimum, a grid track refuses to shrink below its content and
 * the canvas/virtualised lists push the layout past the viewport instead of fitting.
 *
 * The timeframe buttons are presentational by design — the backend only aggregates
 * 1-minute candles, so switching them would change a label and nothing else. "1m" is
 * highlighted and the rest are disabled rather than lying about what they do.
 */
const TIMEFRAMES = ['1m', '15m', '1h', '4h', '1D'] as const;

/**
 * Drawing tools: static. There is no charting engine behind them (lightweight-charts has no
 * drawing primitives), so they are presentational only and deliberately not wired — a
 * toolbar that highlights but draws nothing would be worse than a plainly inert one.
 */
const DRAWING_TOOLS = [
  { glyph: '+', title: 'Crosshair' },
  { glyph: '╱', title: 'Trend line' },
  { glyph: 'T', title: 'Text' },
  { glyph: 'Fib', title: 'Fibonacci retracement' },
  { glyph: '▭', title: 'Rectangle' },
  { glyph: '∥', title: 'Parallel channel' },
] as const;

const POSITIONS_TABS = [
  { id: 'positions', label: 'Positions', count: 0 as number | null },
  { id: 'open-orders', label: 'Open Orders', count: 0 as number | null },
  { id: 'order-history', label: 'Order History', count: null as number | null },
  { id: 'trade-history', label: 'Trade History', count: null as number | null },
] as const;

export function App(): JSX.Element {
  const [activeTab, setActiveTab] = useState<string>('positions');

  return (
    <div className="app">
      <TopBar />

      <main className="app__main">
        <section className="panel panel--chart">
          <div className="chart-toolbar">
            <div className="tf-group" role="group" aria-label="Timeframe">
              {TIMEFRAMES.map((timeframe) => (
                <button
                  key={timeframe}
                  type="button"
                  className={`tf-btn ${timeframe === '1m' ? 'tf-btn--active' : ''}`}
                  disabled={timeframe !== '1m'}
                  title={timeframe === '1m' ? 'Live 1-minute candles' : 'Not aggregated yet'}
                >
                  {timeframe}
                </button>
              ))}
            </div>
            <span className="toolbar-divider" />
            <span className="toolbar-hint">Indicators · none</span>
            <span className="toolbar-divider" />
            <span className="toolbar-hint">BTCUSDT Perpetual · live</span>
          </div>
          {/* chart-shell: 30px drawing rail + the canvas, so the canvas keeps a real box
              for lightweight-charts' own ResizeObserver to measure. */}
          <div className="chart-shell">
            <div className="chart-toolbar-left" aria-label="Drawing tools" role="toolbar">
              {DRAWING_TOOLS.map((tool) => (
                <button
                  key={tool.title}
                  type="button"
                  className="chart-tool"
                  title={`${tool.title} (not implemented)`}
                >
                  {tool.glyph}
                </button>
              ))}
            </div>
            <CandleChart />
          </div>
        </section>

        <section className="panel panel--orderbook">
          <div className="panel__header">
            <span>Order Book</span>
            <span className="panel__hint">@depth20 · 10 Hz</span>
          </div>
          <OrderBook />
        </section>

        <aside className="side-rail">
          <section className="panel panel--order">
            <div className="panel__header">
              <span>Order Entry</span>
              <span className="panel__hint">mock engine</span>
            </div>
            <OrderEntry />
          </section>

          <section className="panel panel--trades">
            <div className="panel__header">
              <span>Market Trades</span>
              <span className="panel__hint">last 50</span>
            </div>
            <TradeFeed />
          </section>
        </aside>
      </main>

      {/* Purely visual: there is no positions/orders read model yet. */}
      <nav className="bottom-bar">
        <div className="bottom-bar__tabs" role="tablist" aria-label="Account panels">
          {POSITIONS_TABS.map((tab) => (
            <button
              key={tab.id}
              type="button"
              role="tab"
              aria-selected={activeTab === tab.id}
              className={`bottom-tab ${activeTab === tab.id ? 'bottom-tab--active' : ''}`}
              onClick={() => setActiveTab(tab.id)}
            >
              {tab.label}
              {tab.count === null ? null : (
                <span className="bottom-tab__count"> ({tab.count})</span>
              )}
            </button>
          ))}
        </div>
        <span className="bottom-bar__hint">display only — no read model yet</span>
      </nav>
    </div>
  );
}

export default App;
