package com.common.Notification.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class AsyncConfig {

    /**
     * Runs Kafka send callbacks off the producer's I/O thread.
     *
     * <p>A producer has a single I/O thread serving every send in the JVM. The completion callback
     * writes to the database, so running it inline would serialise all publishing behind JDBC
     * latency — a throughput ceiling no amount of partitions or replicas can lift — and an
     * exception escaping into that loop can destabilise the producer itself.
     *
     * <p>Sized for the database, not the broker.
     *
     * <p>{@code CallerRunsPolicy} on saturation deliberately pushes back rather than discarding a
     * status update. A dropped {@code markQueued} is not a lost notification — the row stays
     * ACCEPTED and the sweeper republishes it — but that is wasted work worth avoiding.
     */
    @Bean("kafkaCallbackExecutor")
    ThreadPoolTaskExecutor kafkaCallbackExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix("kafka-cb-");
        executor.setCorePoolSize(4);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(1000);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        // Drain in-flight status updates on shutdown instead of stranding rows in ACCEPTED.
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        return executor;
    }
}