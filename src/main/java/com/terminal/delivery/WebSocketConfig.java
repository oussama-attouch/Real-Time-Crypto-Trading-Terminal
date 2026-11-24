package com.terminal.delivery;

import com.terminal.config.DeliveryProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketTransportRegistration;

/**
 * STOMP-over-WebSocket delivery configuration.
 *
 * <h2>Why this does not conflict with the Binance WebFlux client</h2>
 * <p>Step 1 pinned {@code spring.main.web-application-type: servlet}, so the
 * application runs on Tomcat/Spring MVC and this configurer registers its handlers on
 * the servlet {@code DispatcherServlet}. The Binance side is a <b>client</b>
 * ({@code ReactorNettyWebSocketClient}) on its own Netty event loops: it only dials
 * out and never binds a port or registers a route, so the two cannot collide. The
 * failure mode to avoid is the opposite one — adding a reactive server to the
 * classpath, which makes Boot deduce a REACTIVE application and leaves these STOMP
 * endpoints unregistered while the app still starts.
 *
 * <h2>Topology</h2>
 * <pre>
 *   Kafka  ->  TradeKafkaConsumer  ->  SimpMessagingTemplate  ->  /topic/trades
 *                                                                      |
 *                                                        clientOutboundChannel
 *                                                                      v
 *                          browser  <--STOMP/SockJS--  /ws-trades
 * </pre>
 * <p>A "simple broker" is deliberate: one process, fan-out only, no durable
 * subscriptions, no upstream routing. Each connected browser receives only what is
 * published while it is connected, which is exactly right for a live ticker — and it
 * adds no extra hop on the latency path.
 */
@Slf4j
@Configuration
@EnableWebSocketMessageBroker
@RequiredArgsConstructor
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    private final DeliveryProperties properties;

    /**
     * The handshake endpoint. SockJS adds the HTTP fallbacks underneath it
     * ({@code /ws-trades/info}, {@code /xhr_streaming}, ...) because some corporate
     * proxies still break raw WebSocket upgrades. The native transport stays available
     * at {@code /ws-trades/websocket}, which is what {@code @stomp/stompjs} prefers
     * when it can — and is also how a non-browser client (or a curl/Java probe) can
     * speak STOMP without the SockJS framing.
     */
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint(properties.endpoint())
                .setAllowedOriginPatterns(properties.allowedOriginPatterns().toArray(String[]::new))
                .withSockJS();
        log.info("STOMP endpoint registered: {} (+SockJS), origins={}",
                properties.endpoint(), properties.allowedOriginPatterns());
    }

    /**
     * Only two prefixes exist: what clients may send to ({%@code
     * applicationDestinationPrefixes}}) and what the broker may serve
     * ({@code /topic}). Clients get no permission to subscribe to arbitrary
     * destinations, which keeps this from becoming an open relay.
     */
    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.setApplicationDestinationPrefixes(properties.applicationPrefix());

        long heartbeatMillis = properties.heartbeat().toMillis();
        registry.enableSimpleBroker(properties.brokerPrefix())
                // Heartbeats: the server proves liveness on an otherwise idle socket.
                // Without them a half-open connection stays silently "green" in the UI,
                // the worst failure mode for a ticker — it looks like a quiet market
                // instead of a dead link.
                .setHeartbeatValue(new long[]{heartbeatMillis, heartbeatMillis})
                // SimpleBrokerRegistration refuses a heartbeat without a scheduler, so
                // this is mandatory rather than optional. Calling the @Bean method
                // directly is safe (and idiomatic) because @Configuration proxies it: we
                // get the managed singleton, not a second unmanaged scheduler.
                .setTaskScheduler(webSocketHeartbeatScheduler());

        log.info("STOMP broker: app-prefix={} broker-prefix={} trade-topic={} heartbeat={}ms",
                properties.applicationPrefix(), properties.brokerPrefix(),
                properties.tradeTopic(), heartbeatMillis);
    }

    /**
     * Single daemon thread driving STOMP heartbeats for every session.
     *
     * <p>One thread is plenty: a heartbeat is a scheduled write of a single newline per
     * session per interval, and the socket write itself goes through the outbound
     * channel like any other frame. Daemon so it can never hold the JVM open, and
     * {@code waitForTasksToCompleteOnShutdown(false)} so shutdown is not delayed by a
     * pending heartbeat.
     */
    @Bean(name = "webSocketHeartbeatScheduler")
    TaskScheduler webSocketHeartbeatScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("ws-heartbeat-");
        scheduler.setDaemon(true);
        scheduler.setWaitForTasksToCompleteOnShutdown(false);
        return scheduler;
    }

    /**
     * Slow-client protection. Without these, one browser tab that stops draining (frozen
     * VM, closed laptop) accumulates outbound frames and takes the heap with it, and the
     * STOMP session would hold the message indefinitely. Past the limits the session is
     * closed instead — losing one stale client beats losing the JVM.
     */
    @Override
    public void configureWebSocketTransport(WebSocketTransportRegistration registration) {
        registration.setSendBufferSizeLimit(properties.sendBufferSizeLimit())
                .setSendTimeLimit(properties.sendTimeLimit())
                .setMessageSizeLimit(64 * 1024);
    }

    /**
     * Dedicated, bounded executor for writing to clients.
     *
     * <p>This is the delivery-side counterpart of Step 2's {@code publishOn}: the Kafka
     * listener thread hands the message to this channel and returns immediately, so a
     * slow socket can never stall the Kafka consumer. Bounded pool + bounded queue means
     * overload surfaces as a rejection we log, not as unbounded memory growth.
     */
    @Override
    public void configureClientOutboundChannel(ChannelRegistration registration) {
        registration.taskExecutor()
                .corePoolSize(properties.outboundPoolSize())
                .maxPoolSize(properties.outboundPoolSize())
                .queueCapacity(properties.outboundQueueCapacity());
    }
}
