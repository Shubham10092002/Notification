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
            // Read before incrementing. Incrementing unconditionally would let rejected retries
            // keep inflating the counter, so a throttled channel could never recover within its
            // own window — each retry would push the count further past the limit.
            String current = redis.opsForValue().get(redisKey);
            if (current != null && Long.parseLong(current) >= limit) {
                return false;
            }

            Long count = redis.opsForValue().increment(redisKey);
            if (count == null) {
                return true;
            }
            if (count == 1L) {
                // First hit in this window establishes the TTL, so keys cannot accumulate forever.
                redis.expire(redisKey, window);
            }
            // Check-then-increment is not atomic, so concurrent callers can overshoot slightly.
            // Acceptable for protecting a provider quota; a Lua script would make it exact.
            return count <= limit;
        } catch (NumberFormatException ex) {
            log.warn("Unparseable rate limit counter at key={}, allowing request", redisKey);
            return true;
        } catch (RuntimeException ex) {
            // Fail open. A Redis outage must not stop every notification in the platform;
            // losing rate limiting is strictly less bad than losing delivery entirely.
            log.warn("Rate limiter unavailable, allowing request for key={}: {}", key, ex.toString());
            return true;
        }
    }
}