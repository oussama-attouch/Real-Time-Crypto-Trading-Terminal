package com.terminal;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Entry point for Phase 1 of the real-time crypto trading terminal.
 *
 * <p>Runtime shape (single deployable, two logical pipelines):
 * <pre>
 *   Binance WS  --(ReactorNettyWebSocketClient)-->  com.terminal.ingestion  --+
 *                                                                             |
 *                                                       Kafka topic           |
 *                                                       "binance-raw-trades"  |
 *                                                                             v
 *   Browser  <--(STOMP /topic/trades)--  com.terminal.delivery  <--(@KafkaListener)
 * </pre>
 *
 * <p>Cross-cutting configuration lives in {@code com.terminal.config}.
 *
 * <p>{@code @ConfigurationPropertiesScan} is registered up-front so Phase 2 only
 * has to drop a {@code @ConfigurationProperties} class into {@code com.terminal.*}
 * and it will be bound and validated automatically.
 */
@Slf4j
@SpringBootApplication
@ConfigurationPropertiesScan
public class TradingTerminalApplication {

    public static void main(String[] args) {
        SpringApplication.run(TradingTerminalApplication.class, args);
        log.info("Trading terminal started :: phase=1 (Binance -> Kafka -> STOMP)");
    }
}
