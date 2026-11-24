package com.terminal.config;

import io.netty.channel.ChannelOption;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.WebsocketClientSpec;

import java.time.Clock;

/**
 * Wires the reactive WebSocket <em>client</em> used to consume Binance.
 *
 * <p>Everything here is hand-built rather than auto-configured, on purpose:
 * {@code WebClient.Builder} auto-configuration is not guaranteed on a servlet
 * stack, and {@code ReactorNettyWebSocketClient} is not auto-configured at all.
 * Explicit construction also means the timeouts, the frame limit and the ping
 * handling are visible in code instead of implied by a property nobody remembers.
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
public class IngestionConfig {

    /**
     * Dedicated, bounded scheduler for the {@code KafkaProducer.send()} step.
     *
     * <p>Why not the global {@code Schedulers.boundedElastic()}? Because a shared
     * pool means a stall in an unrelated component can starve the market-data
     * publisher. This instance is owned by us, named, disposable, and daemon so it
     * cannot hold the JVM open at shutdown.
     *
     * <p>{@code queueCapacity} is a real bound: when it is exhausted the scheduler
     * rejects work and Reactor signals the overflow instead of growing a queue
     * forever behind a dead broker.
     */
    @Bean(name = "binanceIngestScheduler", destroyMethod = "dispose")
    Scheduler binanceIngestScheduler(BinanceProperties properties) {
        BinanceProperties.SchedulerSettings cfg = properties.scheduler();
        Scheduler scheduler = Schedulers.newBoundedElastic(
                cfg.poolSize(),
                cfg.queueCapacity(),
                cfg.threadPrefix(),
                60,      // keep threads 60s before reclaiming
                true);   // daemon
        log.info("Binance ingest scheduler: pool={} queue={} threads={}-N",
                cfg.poolSize(), cfg.queueCapacity(), cfg.threadPrefix());
        return scheduler;
    }

    /**
     * A dedicated Netty {@link HttpClient} for the market-data socket.
     *
     * <p>Kept separate from any HTTP client Boot would build, so this connection
     * pool and its timeouts are ours alone and cannot be perturbed by, say, a
     * later REST call to a different exchange.
     */
    @Bean
    HttpClient binanceHttpClient(BinanceProperties properties) {
        int connectTimeoutMs = Math.toIntExact(properties.connectTimeout().toMillis());
        return HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
                // Measured to the HTTP 101 Switching Protocols response, i.e. the
                // WebSocket handshake. Without it a half-open TCP connect hangs for
                // the OS default (~2 min) and delays every subsequent retry.
                .responseTimeout(properties.handshakeTimeout())
                // WebSocket frames are not content-encoded; asking for gzip only
                // adds negotiation risk.
                .compress(false)
                .keepAlive(true);
    }

    /**
     * {@code handlePing(true)} is load-bearing, not decoration: Binance sends a
     * protocol-level ping every 3 minutes and disconnects any client that fails to
     * answer with a pong for 10 minutes. With this flag Reactor Netty replies
     * automatically, and because it is handled below the application layer it can
     * never be starved by work in our own operators.
     *
     * <p>Both settings go through the {@link WebsocketClientSpec} supplier rather
     * than {@code setMaxFramePayloadLength}/{@code setHandlePing}: those setters are
     * deprecated as of Spring Framework 6.2. The supplier is invoked once per
     * {@code execute()}, so a fresh (mutable) builder is returned each time.
     */
    // Named reactorNettyWebSocketClient, NOT binanceWebSocketClient: the
    // @Component com.terminal.ingestion.BinanceWebSocketClient already owns that
    // bean name, and a collision is a hard startup failure
    // (BeanDefinitionOverrideException) since bean overriding stays disabled.
    @Bean
    ReactorNettyWebSocketClient reactorNettyWebSocketClient(HttpClient binanceHttpClient,
                                                            BinanceProperties properties) {
        int maxFramePayloadLength = properties.maxFramePayloadLength();
        return new ReactorNettyWebSocketClient(binanceHttpClient,
                () -> WebsocketClientSpec.builder()
                        .maxFramePayloadLength(maxFramePayloadLength)
                        .handlePing(true));
    }

    /**
     * Injected (rather than calling {@code System.currentTimeMillis()}) so the
     * receipt timestamp is controllable in tests.
     */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
