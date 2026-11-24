import { useTradingStore } from '../store/tradingStore';
import { wsService } from './wsService';

let started = false;

/**
 * Wires transport to state. Started once from `main.tsx`.
 *
 * Deliberately NOT in a React effect: `<StrictMode>` double-invokes effects in
 * development, so a `useEffect(() => start())` would run twice. The module-level flag
 * makes this idempotent, and `wsService.connect()` is guarded as well
 * (`if (this.client !== null) return;`), so a double call cannot open two sockets.
 *
 * Every handler reads `getState()` at call time rather than capturing a snapshot, so the
 * store reference is always the current one — including after a hot reload.
 */
export function start(): void {
  if (started) {
    return;
  }
  started = true;

  wsService.onStatus((connected) => {
    useTradingStore.getState().setConnected(connected);
  });

  wsService.onSnapshot((snapshot) => {
    useTradingStore.getState().applySnapshot(snapshot);
  });

  // These three only append to buffers; the store commits them at 20 Hz (see
  // tradingStore.flush), so socket rate never becomes render rate.
  wsService.onTrade((trade) => {
    useTradingStore.getState().ingestTrade(trade);
  });

  wsService.onLiveCandle((candle) => {
    useTradingStore.getState().ingestLiveCandle(candle);
  });

  wsService.onClosedCandle((candle) => {
    useTradingStore.getState().ingestClosedCandle(candle);
  });

  wsService.onOrderBook((snapshot) => {
    useTradingStore.getState().ingestOrderBook(snapshot);
  });

  /**
   * NOT buffered like trades/candles. Those are coalescable — only the newest book matters,
   * and 2,000 trades collapse into one render. An order confirmation is an individual fact:
   * buffer it and two orders placed in the same 50 ms window would collapse into one
   * notification. It goes straight into the store.
   */
  wsService.onOrder((order) => {
    useTradingStore.getState().ingestOrderEvent(order);
  });

  wsService.connect();
}

export function stop(): void {
  wsService.disconnect();
  started = false;
}
