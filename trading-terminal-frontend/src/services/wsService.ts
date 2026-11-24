import { Client, type IMessage, type IStompSocket } from '@stomp/stompjs';
import SockJS from 'sockjs-client';
import {
  DESTINATIONS,
  type CandleEvent,
  type OrderBookSnapshot,
  type OrderPlacedEvent,
  type TradeEvent,
} from '../types/trade';

type Unsubscribe = () => void;
type PayloadHandler<T> = (payload: T) => void;
type StatusHandler = (connected: boolean) => void;

/**
 * Relative on purpose: the browser talks to the Vite origin and the dev proxy
 * forwards to Spring, so there is no CORS setup and no localhost:8080 in the bundle.
 */
const ENDPOINT = '/ws-trades';
const RECONNECT_DELAY_MS = 5_000;

/**
 * Transport only. It parses frames and hands typed payloads to whoever registered;
 * it knows nothing about React or Zustand (the store bridge does the wiring), which
 * keeps it testable and makes the render-rate decision live in exactly one place.
 */
export class WsService {
  private client: Client | null = null;

  private readonly snapshotHandlers = new Set<PayloadHandler<TradeEvent[]>>();
  private readonly tradeHandlers = new Set<PayloadHandler<TradeEvent>>();
  private readonly liveCandleHandlers = new Set<PayloadHandler<CandleEvent>>();
  private readonly closedCandleHandlers = new Set<PayloadHandler<CandleEvent>>();
  private readonly statusHandlers = new Set<StatusHandler>();
  private readonly orderBookHandlers = new Set<PayloadHandler<OrderBookSnapshot>>();
  private readonly orderHandlers = new Set<PayloadHandler<OrderPlacedEvent>>();

  /** Idempotent: calling twice does not open a second socket. */
  connect(): void {
    if (this.client !== null) {
      return;
    }

    const client = new Client({
      // SockJS first, raw WebSocket underneath where possible.
      webSocketFactory: () => new SockJS(ENDPOINT) as unknown as IStompSocket,
      reconnectDelay: RECONNECT_DELAY_MS,
      heartbeatIncoming: 10_000,
      heartbeatOutgoing: 10_000,
      debug: () => {
        /* flip to console.debug to inspect raw frames */
      },
    });

    // Fires on the first connect AND on every reconnect, which is exactly what makes
    // this the right place to (re)subscribe: STOMP subscriptions do not survive a
    // dropped connection.
    client.onConnect = () => {
      this.emitStatus(true);
      this.subscribeAll(client);
    };
    client.onWebSocketClose = () => this.emitStatus(false);
    client.onWebSocketError = () => this.emitStatus(false);
    client.onStompError = (frame) => {
      // eslint-disable-next-line no-console
      console.error('[wsService] STOMP error:', frame.headers.message, frame.body);
      this.emitStatus(false);
    };

    this.client = client;
    this.emitStatus(false);
    client.activate();
  }

  disconnect(): void {
    const client = this.client;
    this.client = null;
    if (client !== null) {
      void client.deactivate();
    }
    this.emitStatus(false);
  }

  onStatus(handler: StatusHandler): Unsubscribe {
    this.statusHandlers.add(handler);
    return () => this.statusHandlers.delete(handler);
  }

  /** Last 50 trades, newest first, delivered once per (re)connect. */
  onSnapshot(handler: PayloadHandler<TradeEvent[]>): Unsubscribe {
    this.snapshotHandlers.add(handler);
    return () => this.snapshotHandlers.delete(handler);
  }

  onTrade(handler: PayloadHandler<TradeEvent>): Unsubscribe {
    this.tradeHandlers.add(handler);
    return () => this.tradeHandlers.delete(handler);
  }

  /** In-progress 1m candle, pushed on every trade. */
  onLiveCandle(handler: PayloadHandler<CandleEvent>): Unsubscribe {
    this.liveCandleHandlers.add(handler);
    return () => this.liveCandleHandlers.delete(handler);
  }

  /** Finalized 1m candle. May repeat for the same openTime when a late trade corrects it. */
  onClosedCandle(handler: PayloadHandler<CandleEvent>): Unsubscribe {
    this.closedCandleHandlers.add(handler);
    return () => this.closedCandleHandlers.delete(handler);
  }

  /** L2 order book snapshot, already throttled to 10 Hz server-side. */
  onOrderBook(handler: PayloadHandler<OrderBookSnapshot>): Unsubscribe {
    this.orderBookHandlers.add(handler);
    return () => this.orderBookHandlers.delete(handler);
  }

  /**
   * Order confirmed by the read side (Postgres -> Kafka -> STOMP). Distinct from the
   * `201` that `POST /api/orders` returns: that is the command being accepted, this is the
   * event having round-tripped the log.
   */
  onOrder(handler: PayloadHandler<OrderPlacedEvent>): Unsubscribe {
    this.orderHandlers.add(handler);
    return () => this.orderHandlers.delete(handler);
  }

  private subscribeAll(client: Client): void {
    // The @SubscribeMapping reply is a single JSON array on this subscription.
    client.subscribe(DESTINATIONS.snapshot, (message: IMessage) => {
      const batch = this.parse<TradeEvent[]>(message, DESTINATIONS.snapshot);
      if (batch !== null) {
        this.dispatch(this.snapshotHandlers, batch);
      }
    });

    client.subscribe(DESTINATIONS.trades, (message: IMessage) => {
      const trade = this.parse<TradeEvent>(message, DESTINATIONS.trades);
      if (trade !== null) {
        this.dispatch(this.tradeHandlers, trade);
      }
    });

    client.subscribe(DESTINATIONS.liveCandle, (message: IMessage) => {
      const candle = this.parse<CandleEvent>(message, DESTINATIONS.liveCandle);
      if (candle !== null) {
        this.dispatch(this.liveCandleHandlers, candle);
      }
    });

    client.subscribe(DESTINATIONS.orderBook, (message: IMessage) => {
      const book = this.parse<OrderBookSnapshot>(message, DESTINATIONS.orderBook);
      if (book !== null) {
        this.dispatch(this.orderBookHandlers, book);
      }
    });

    client.subscribe(DESTINATIONS.orders, (message: IMessage) => {
      const order = this.parse<OrderPlacedEvent>(message, DESTINATIONS.orders);
      if (order !== null) {
        this.dispatch(this.orderHandlers, order);
      }
    });

    client.subscribe(DESTINATIONS.closedCandles, (message: IMessage) => {
      const candle = this.parse<CandleEvent>(message, DESTINATIONS.closedCandles);
      if (candle !== null) {
        this.dispatch(this.closedCandleHandlers, candle);
      }
    });
  }

  /** One malformed frame must not tear down the subscription. */
  private parse<T>(message: IMessage, destination: string): T | null {
    try {
      return JSON.parse(message.body) as T;
    } catch (error) {
      // eslint-disable-next-line no-console
      console.error(`[wsService] unparseable frame on ${destination}:`, error, message.body);
      return null;
    }
  }

  private dispatch<T>(handlers: Set<PayloadHandler<T>>, payload: T): void {
    for (const handler of handlers) {
      try {
        handler(payload);
      } catch (error) {
        // A broken consumer must not stop the others.
        // eslint-disable-next-line no-console
        console.error('[wsService] handler threw:', error);
      }
    }
  }

  private emitStatus(connected: boolean): void {
    for (const handler of this.statusHandlers) {
      handler(connected);
    }
  }
}

export const wsService = new WsService();
