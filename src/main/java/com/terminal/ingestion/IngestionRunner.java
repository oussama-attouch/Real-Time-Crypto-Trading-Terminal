package com.terminal.ingestion;

import com.terminal.config.BinanceProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;

/**
 * Bootstraps the ingestion pipeline.
 *
 * <p>{@link ApplicationRunner} rather than {@code @PostConstruct}: the socket must
 * open <em>after</em> the context is fully refreshed, not while beans are still
 * being created, so a slow or hanging handshake can never block startup.
 *
 * <p>{@link DisposableBean} closes the loop: {@code destroy()} runs during graceful
 * shutdown and cancels the subscription, which also discards a pending backoff timer
 * — shutdown never waits out a 30s reconnect delay. Bean destruction happens before
 * the Kafka producer factory is torn down, so in-flight publishes are not cut off
 * mid-flight.
 *
 * <p>{@code connect()} returns a cold {@link reactor.core.publisher.Mono}: this runner
 * is the single subscriber, and it never blocks on it — subscription is asynchronous,
 * so a Binance outage delays nothing at startup.
 */
@Slf4j
@Component
public class IngestionRunner implements ApplicationRunner, DisposableBean {

    private final BinanceWebSocketClient client;
    private final BinanceProperties properties;

    /** Volatile: written by the Spring startup thread, read by the shutdown thread. */
    private volatile Disposable subscription;

    public IngestionRunner(BinanceWebSocketClient client, BinanceProperties properties) {
        this.client = client;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!properties.enabled()) {
            log.info("Binance ingestion DISABLED (terminal.binance.enabled=false); no socket opened");
            return;
        }
        log.info("Ingestion runner starting Binance WebSocket stream");
        this.subscription = client.connect().subscribe(
                ignored -> { },
                error -> log.error("Ingestion stream TERMINATED; no further reconnects: {}", error.toString()),
                () -> log.warn("Ingestion stream completed; ingestion is no longer running"));
    }

    @Override
    public void destroy() {
        Disposable current = this.subscription;
        this.subscription = null;
        if (current != null && !current.isDisposed()) {
            current.dispose();
            log.info("Ingestion runner stopped the Binance WebSocket stream");
        }
    }
}
