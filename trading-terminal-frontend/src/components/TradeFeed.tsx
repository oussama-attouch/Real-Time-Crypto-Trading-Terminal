import { useEffect, useRef, useState } from 'react';
import { FixedSizeList, type ListChildComponentProps } from 'react-window';
import { useTradingStore } from '../store/tradingStore';
import type { TradeEvent } from '../types/trade';
import { formatPrice, formatQty } from '../utils/format';

const ROW_HEIGHT = 20;

function formatTime(epochMillis: number): string {
  const date = new Date(epochMillis);
  const pad = (value: number, width: number) => String(value).padStart(width, '0');
  return `${pad(date.getHours(), 2)}:${pad(date.getMinutes(), 2)}:${pad(date.getSeconds(), 2)}.${pad(
    date.getMilliseconds(),
    3,
  )}`;
}

/**
 * Row is defined at module scope and gets the trade list through `itemData`, so
 * react-window can reuse it instead of seeing a brand-new component type on every
 * parent render (which would remount every visible row).
 */
function Row({ index, style, data }: ListChildComponentProps<TradeEvent[]>): JSX.Element {
  const trade = data[index];
  if (trade === undefined) {
    // `noUncheckedIndexedAccess` is on, so an out-of-range index is a type-level
    // possibility. react-window should never ask for one; render a blank row rather
    // than throw if it ever does.
    return <div className="trade-row" style={style} />;
  }

  // Binance `m` means "buyer is maker": the aggressor was the seller.
  const isSell = trade.isBuyerMaker;

  return (
    <div className="trade-row" style={style}>
      <span className="trade-row__time">{formatTime(trade.tradeTime)}</span>
      <span className={`trade-row__side ${isSell ? 'is-sell' : 'is-buy'}`}>
        {isSell ? 'SELL' : 'BUY'}
      </span>
      {/* Formatted for display only; the exact strings stay in the store. */}
      <span className="trade-row__price">{formatPrice(trade.price)}</span>
      <span className="trade-row__qty">{formatQty(trade.quantity)}</span>
    </div>
  );
}

export function TradeFeed(): JSX.Element {
  const trades = useTradingStore((state) => state.trades);

  const containerRef = useRef<HTMLDivElement | null>(null);
  const [size, setSize] = useState({ width: 0, height: 0 });

  // react-window needs pixel dimensions; measure the panel rather than hard-coding.
  useEffect(() => {
    const element = containerRef.current;
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

  return (
    <div className="trade-feed">
      <div className="trade-feed__header">
        <span>Time</span>
        <span>Side</span>
        <span className="align-right">Price</span>
        <span className="align-right">Quantity</span>
      </div>

      <div className="trade-feed__body" ref={containerRef}>
        {size.height > 0 && trades.length > 0 ? (
          /**
           * Virtualised even though the store caps the list at 50: this is the shape the
           * component must have once the cap moves to thousands of rows (or once a
           * second feed â€” ETH, SOL â€” is added). Only the ~20 visible rows are ever
           * mounted, so a 20 Hz store flush costs a handful of DOM writes.
           */
          <FixedSizeList
            height={size.height}
            width={size.width}
            itemCount={trades.length}
            itemSize={ROW_HEIGHT}
            itemData={trades}
            itemKey={(index, data) => data[index]?.tradeId ?? index}
            overscanCount={6}
          >
            {Row}
          </FixedSizeList>
        ) : (
          <div className="trade-feed__empty">waiting for tradesâ€¦</div>
        )}
      </div>
    </div>
  );
}
