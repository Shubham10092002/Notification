package com.common.Notification.ratelimit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RedisRateLimiterTest {

    private static final Duration WINDOW = Duration.ofMinutes(1);

    @Mock
    private StringRedisTemplate redis;
    @Mock
    private ValueOperations<String, String> valueOps;

    private RedisRateLimiter rateLimiter;

    @BeforeEach
    void setUp() {
        when(redis.opsForValue()).thenReturn(valueOps);
        rateLimiter = new RedisRateLimiter(redis);
    }

    @Test
    @DisplayName("allows a request below the limit and sets a TTL on the first hit")
    void allowsBelowLimit() {
        when(valueOps.get(anyString())).thenReturn(null);
        when(valueOps.increment(anyString())).thenReturn(1L);

        assertThat(rateLimiter.tryAcquire("channel:EMAIL", 10, WINDOW)).isTrue();
        verify(redis).expire(anyString(), eqWindow());
    }

    @Test
    @DisplayName("a rejected request does not increment the counter")
    void doesNotCompoundOnRejection() {
        when(valueOps.get(anyString())).thenReturn("10");

        assertThat(rateLimiter.tryAcquire("channel:SMS", 10, WINDOW)).isFalse();

        // Regression guard: incrementing on rejection let retries push the counter ever further
        // past the limit, so a throttled channel could never recover inside its own window.
        verify(valueOps, never()).increment(anyString());
    }

    @Test
    @DisplayName("fails open when Redis is unreachable")
    void failsOpenOnRedisOutage() {
        when(valueOps.get(anyString())).thenThrow(new RedisConnectionFailureException("down"));

        // Losing rate limiting is strictly better than losing every notification.
        assertThat(rateLimiter.tryAcquire("channel:EMAIL", 10, WINDOW)).isTrue();
    }

    @Test
    @DisplayName("fails open on a corrupt counter value rather than blocking delivery")
    void failsOpenOnCorruptCounter() {
        when(valueOps.get(anyString())).thenReturn("not-a-number");

        assertThat(rateLimiter.tryAcquire("channel:EMAIL", 10, WINDOW)).isTrue();
    }

    @Test
    @DisplayName("the request that reaches the limit is still allowed; the next one is not")
    void allowsExactlyTheLimit() {
        when(valueOps.get(anyString())).thenReturn("9");
        when(valueOps.increment(anyString())).thenReturn(10L);

        assertThat(rateLimiter.tryAcquire("channel:EMAIL", 10, WINDOW)).isTrue();
    }

    private static Duration eqWindow() {
        return org.mockito.ArgumentMatchers.eq(WINDOW);
    }
}