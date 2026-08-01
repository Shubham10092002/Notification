package com.common.Notification.config;

import com.common.Notification.exception.NotificationDeliveryException;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Circuit breaker policy for provider calls.
 *
 * <p>Uses the Resilience4j core library directly rather than its Spring Boot starter, which is
 * published against Boot 3 — the decorator needs only a {@code CircuitBreaker} instance, so the
 * Spring integration buys nothing here.
 */
@Configuration
public class ResilienceConfig {

    @Bean
    CircuitBreakerRegistry circuitBreakerRegistry(
            @Value("${notification.circuit-breaker.sliding-window-size:20}") int slidingWindowSize,
            @Value("${notification.circuit-breaker.minimum-calls:10}") int minimumCalls,
            @Value("${notification.circuit-breaker.failure-rate-threshold:50}") float failureRate,
            @Value("${notification.circuit-breaker.open-seconds:30}") long openSeconds,
            @Value("${notification.circuit-breaker.half-open-calls:3}") int halfOpenCalls) {

        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(slidingWindowSize)
                // Do not trip on a handful of calls at startup or during a quiet period.
                .minimumNumberOfCalls(minimumCalls)
                .failureRateThreshold(failureRate)
                .waitDurationInOpenState(Duration.ofSeconds(openSeconds))
                .permittedNumberOfCallsInHalfOpenState(halfOpenCalls)
                // No idle consumer thread is guaranteed to probe the provider, so the breaker
                // must move itself to half-open on a timer rather than waiting for a call.
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                // Only provider failures count. A configuration mistake or a bug in our own code
                // should surface as an error, not silently trip the breaker for everyone.
                .recordExceptions(NotificationDeliveryException.class)
                .build();

        return CircuitBreakerRegistry.of(config);
    }
}