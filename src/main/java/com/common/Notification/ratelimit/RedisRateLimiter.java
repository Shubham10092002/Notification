package com.common.Notification.ratelimit;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Fixed-window rate limiter backed by Redis (the doc's "Rate Limiter" concept).
 *
 * <p>Shared across every instance of the service, which is the point: a per-JVM limiter would
 * let N replicas send N times the provider's allowance and get the account throttled or banned.
 *
 * <p>Fixed window is used rather than sliding window because it costs a single INCR. The known
 * trade-off is burst tolerance at a window boundary (up to 2x the limit across two adjacent
 * windows); acceptable for protecting a provider quota, not sufficient for billing-grade limits.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RedisRateLimiter {

    private final StringRedisTemplate redis;

    /**
     * @return true if the caller may proceed, false if this window's budget is spent.
     */
    public boolean tryAcquire(String key, int limit, Duration window) {
        String redisKey = "ratelimit:" + key + ":" + (System.currentTimeMillis() / window.toMillis());
        try {
            Long count = redis.opsForValue().increment(redisKey);
            if (count == null) {
                return true;
            }
            if (count == 1L) {
                // First hit in this window establishes the TTL, so keys cannot accumulate forever.
                redis.expire(redisKey, window);
            }
            return count <= limit;
        } catch (RuntimeException ex) {
            // Fail open. A Redis outage must not stop every notification in the platform;
            // losing rate limiting is strictly less bad than losing delivery entirely.
            log.warn("Rate limiter unavailable, allowing request for key={}: {}", key, ex.toString());
            return true;
        }
    }
}