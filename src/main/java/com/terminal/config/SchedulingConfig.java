package com.terminal.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Scheduling infrastructure, deliberately isolated in its own configuration class.
 *
 * <h2>Why this cannot live on the bean that uses @Scheduled</h2>
 * <p>{@code @EnableScheduling} registers {@code ScheduledAnnotationBeanPostProcessor}, a
 * {@link org.springframework.beans.factory.config.BeanPostProcessor}. Bean post-processors
 * are instantiated in the very first phase of context refresh, and this one needs the
 * {@code TaskScheduler} to register tasks — so any {@code @Bean} of that type is created
 * abnormally early.
 *
 * <p>Declaring {@code @EnableScheduling} and the scheduler bean on
 * {@code OrderBookBroadcaster} (which injects {@code SimpMessagingTemplate}) therefore
 * created a cycle the container cannot resolve without proxies:
 * <pre>
 *   ScheduledAnnotationBeanPostProcessor
 *     -> taskScheduler (@Bean on OrderBookBroadcaster)
 *       -> OrderBookBroadcaster
 *         -> SimpMessagingTemplate (brokerMessagingTemplate)
 *           -> ...broker configuration, not yet initialised
 * </pre>
 * That surfaced only at startup as
 * "Requested bean is currently in creation: Is there an unresolvable circular reference?".
 *
 * <p>This class has no dependencies at all, so the post-processor can be built from it
 * without pulling anything else into the early phase.
 *
 * <p>The bean is named {@code taskScheduler} on purpose:
 * {@code ScheduledAnnotationBeanPostProcessor} prefers a {@code TaskScheduler} bean by that
 * name, which keeps the 10 Hz order-book broadcast off the single-threaded STOMP heartbeat
 * scheduler declared in {@code WebSocketConfig}.
 */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
public class SchedulingConfig {

    @Bean(name = "taskScheduler", destroyMethod = "shutdown")
    ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        // One scheduled task today, but serialization must never queue behind itself.
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("sched-");
        scheduler.setDaemon(true);
        scheduler.setWaitForTasksToCompleteOnShutdown(false);
        return scheduler;
    }
}
