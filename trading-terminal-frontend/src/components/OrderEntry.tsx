import { useMemo, useState } from 'react';
import { useTradingStore } from '../store/tradingStore';

type Side = 'BUY' | 'SELL';
type OrderType = 'LIMIT' | 'MARKET';

interface Toast {
  kind: 'success' | 'error';
  message: string;
}

interface PlaceOrderResponse {
  orderId: string;
  symbol: string;
  side: string;
  price: string;
  quantity: string;
  status: string;
  createdAt: number;
}

const SYMBOL = 'BTCUSDT';

/**
 * Mock account size. There is no balances service yet, so the slider sizes an order as a
 * percentage of this notional. It exists to make the control real (it writes quantity)
 * rather than decorative â€” swap it for the account snapshot when one exists.
 */
const AVAILABLE_USDT = 5_000;

/**
 * Order ticket: Limit/Market tabs, price + size, percentage slider, and one solid button
 * per side.
 *
 * Decimals leave this component as the exact strings the user typed. `Number()` is used
 * only to validate and to size the slider; the payload itself is never numeric, so the
 * value that reaches `BigDecimal` on the server is the value on screen.
 *
 * A MARKET order omits `price` entirely instead of sending `""`: the column is nullable
 * precisely because a market order has no limit price, and an empty string would fail
 * BigDecimal binding with a 400 rather than meaning "no price".
 */
export function OrderEntry(): JSX.Element {
  const [orderType, setOrderType] = useState<OrderType>('LIMIT');
  const [price, setPrice] = useState('');
  const [quantity, setQuantity] = useState('');
  const [percent, setPercent] = useState(0);
  const [submittingSide, setSubmittingSide] = useState<Side | null>(null);
  const [toast, setToast] = useState<Toast | null>(null);

  const lastPrice = useTradingStore((state) => state.lastPrice);
  // Primitive selectors, not one object selector: Zustand compares with Object.is, so an
  // object selector would re-render this panel on every 20 Hz store flush.
  const lastOrderEvent = useTradingStore((state) => state.lastOrderEvent);
  const clearOrderEvent = useTradingStore((state) => state.clearOrderEvent);

  // Reference price for the slider: the limit price, or the live price for a market order.
  const referencePrice = useMemo(() => {
    const typed = Number(price);
    if (Number.isFinite(typed) && typed > 0) {
      return typed;
    }
    const live = Number(lastPrice);
    return Number.isFinite(live) && live > 0 ? live : null;
  }, [price, lastPrice]);

  const priceValid = Number(price) > 0;
  const quantityValid = Number(quantity) > 0;
  const canSubmit =
    submittingSide === null && quantityValid && (orderType === 'MARKET' || priceValid);

  /**
   * Once the STOMP event for this order comes back, the "placed successfully" toast is
   * redundant — same order, stronger guarantee. Deriving it here (rather than clearing the
   * toast in an effect) keeps it a pure render decision and avoids a state update per event.
   */
  const supersededByEvent =
    toast !== null && lastOrderEvent !== null && toast.message.includes(lastOrderEvent.orderId);

  const sizeFromPercent = (nextPercent: number): string => {
    if (referencePrice === null) {
      return '';
    }
    const notional = (nextPercent / 100) * AVAILABLE_USDT;
    return (notional / referencePrice).toFixed(3);
  };

  const handleSlider = (nextPercent: number): void => {
    setPercent(nextPercent);
    const nextQuantity = sizeFromPercent(nextPercent);
    if (nextQuantity !== '') {
      setQuantity(nextQuantity);
    }
  };

  const handleTypeChange = (next: OrderType): void => {
    setOrderType(next);
    setToast(null);
    // Percent sizing in a market order is an estimate at the live price; reset it so a
    // stale number never looks confirmed.
    setPercent(0);
  };

  async function submit(side: Side): Promise<void> {
    if (!canSubmit) {
      return;
    }

    setSubmittingSide(side);
    setToast(null);

    try {
      const body: Record<string, string> = { symbol: SYMBOL, side, quantity };
      if (orderType === 'LIMIT') {
        body.price = price;
      }

      const response = await fetch('/api/orders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });

      if (!response.ok) {
        throw new Error(await describeFailure(response));
      }

      const created = (await response.json()) as PlaceOrderResponse;
      setToast({ kind: 'success', message: `Order placed successfully: ${created.orderId}` });
      setQuantity('');
      setPercent(0);
    } catch (error) {
      setToast({
        kind: 'error',
        message: error instanceof Error ? error.message : 'Order failed',
      });
    } finally {
      setSubmittingSide(null);
    }
  }

  return (
    <form className="order-entry" onSubmit={(event) => event.preventDefault()} noValidate>
      {/* Leverage / margin mode. Purely visual: the mock engine has no margin model. */}
      <div className="oe-leverage">
        <button type="button" className="oe-lev-btn" title="Margin mode">
          Cross
        </button>
        <span className="oe-lev-divider" />
        <button type="button" className="oe-lev-btn" title="Leverage">
          20x
        </button>
      </div>
      <div className="oe-tabs" role="tablist" aria-label="Order type">
        {(['LIMIT', 'MARKET'] as const).map((type) => (
          <button
            key={type}
            type="button"
            role="tab"
            aria-selected={orderType === type}
            className={`oe-tab ${orderType === type ? 'oe-tab--active' : ''}`}
            onClick={() => handleTypeChange(type)}
          >
            {type === 'LIMIT' ? 'Limit' : 'Market'}
          </button>
        ))}
      </div>

      <div className="oe-balance">
        <span>Available</span>
        <span className="oe-balance__value">
          {AVAILABLE_USDT.toLocaleString('en-US', { minimumFractionDigits: 2 })} USDT
        </span>
      </div>

      <div className="oe-body">
        <div className="oe-field">
          <span className="oe-field__label">
            Price {orderType === 'MARKET' ? '(market)' : ''}
          </span>
          <div className="oe-input-wrap">
            <input
              className="oe-input"
              type="text"
              inputMode="decimal"
              placeholder={orderType === 'MARKET' ? 'best available' : referencePrice?.toFixed(2) ?? '0.00'}
              value={orderType === 'MARKET' ? '' : price}
              onChange={(event) => setPrice(event.target.value)}
              disabled={orderType === 'MARKET'}
              autoComplete="off"
              aria-label="Limit price"
            />
            <span className="oe-unit">USDT</span>
          </div>
        </div>

        <div className="oe-field">
          <span className="oe-field__label">Size</span>
          <div className="oe-input-wrap">
            <input
              className="oe-input"
              type="text"
              inputMode="decimal"
              placeholder="0.000"
              value={quantity}
              onChange={(event) => setQuantity(event.target.value)}
              autoComplete="off"
              aria-label="Order size"
            />
            <span className="oe-unit">BTC</span>
          </div>
        </div>

        <div className="oe-slider">
          <input
            type="range"
            min={0}
            max={100}
            step={25}
            value={percent}
            onChange={(event) => handleSlider(Number(event.target.value))}
            disabled={referencePrice === null}
            aria-label="Size as a percentage of available balance"
          />
          <div className="oe-slider__marks">
            <span>0%</span>
            <span>25%</span>
            <span>50%</span>
            <span>75%</span>
            <span>100%</span>
          </div>
        </div>

        {/* Both buttons submit explicitly: the side is a property of the click, not of
            form state, so there is no hidden "selected side" to get out of sync. */}
        <div className="oe-actions">
          <button
            type="button"
            className="btn btn--buy"
            disabled={!canSubmit}
            aria-busy={submittingSide === 'BUY'}
            onClick={() => void submit('BUY')}
          >
            {submittingSide === 'BUY' ? <span className="spinner" aria-hidden="true" /> : null}
            Buy / Long
          </button>

          <button
            type="button"
            className="btn btn--sell"
            disabled={!canSubmit}
            aria-busy={submittingSide === 'SELL'}
            onClick={() => void submit('SELL')}
          >
            {submittingSide === 'SELL' ? <span className="spinner" aria-hidden="true" /> : null}
            Sell / Short
          </button>
        </div>
      </div>

      <div className="order-entry__toast-slot" role="status" aria-live="polite">
        {toast !== null && !supersededByEvent ? (
          <div className={`toast toast--${toast.kind}`}>
            {toast.message}
            <button
              type="button"
              className="toast__close"
              onClick={() => setToast(null)}
              aria-label="Dismiss"
            >
              ×
            </button>
          </div>
        ) : null}

        {/* Round-tripped confirmation: Postgres -> Kafka -> /topic/orders. */}
        {lastOrderEvent !== null ? (
          <div className="toast toast--success">
            Order confirmed via WS: {lastOrderEvent.orderId}
            <button
              type="button"
              className="toast__close"
              onClick={clearOrderEvent}
              aria-label="Dismiss"
            >
              ×
            </button>
          </div>
        ) : null}
      </div>
    </form>
  );
}

/**
 * Turns a failed response into something a human can act on.
 *
 * 400 carries per-field messages from Bean Validation; 503 is the database being
 * unreachable and is explicitly retryable, which is why it must not read like a rejection.
 */
async function describeFailure(response: Response): Promise<string> {
  if (response.status === 503) {
    return 'Order store unavailable â€” please retry in a moment.';
  }

  let detail = '';
  try {
    const problem = (await response.json()) as {
      detail?: string;
      errors?: Record<string, string>;
    };
    detail =
      problem.errors === undefined
        ? problem.detail ?? ''
        : Object.entries(problem.errors)
            .map(([field, message]) => `${field}: ${message}`)
            .join(', ');
  } catch {
    // Non-JSON error body: fall through to the status text.
  }

  return detail === '' ? `Order rejected (HTTP ${response.status})` : `Order rejected â€” ${detail}`;
}
