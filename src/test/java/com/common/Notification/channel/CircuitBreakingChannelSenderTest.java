package com.common.Notification.channel;

import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationRecord;
import com.common.Notification.exception.NotificationDeliveryException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CircuitBreakingChannelSenderTest {

    @Mock
    private ChannelSender delegate;

    private CircuitBreaker circuitBreaker;
    private CircuitBreakingChannelSender sender;

    @BeforeEach
    void setUp() {
        when(delegate.channel()).thenReturn(Channel.EMAIL);
        circuitBreaker = CircuitBreaker.of("test", CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(4)
                .minimumNumberOfCalls(4)
                .failureRateThreshold(50f)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .recordExceptions(NotificationDeliveryException.class)
                .build());
        sender = new CircuitBreakingChannelSender(delegate, circuitBreaker);
    }

    private NotificationRecord record() {
        return NotificationRecord.accept("req-1", "svc", Channel.EMAIL,
                "jane@example.com", "WELCOME", "Hi", "Hello");
    }

    @Test
    @DisplayName("passes through to the delegate while the breaker is closed")
    void delegatesWhenClosed() {
        NotificationRecord record = record();

        sender.send(record);

        verify(delegate).send(record);
        assertThat(circuitBreaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    @DisplayName("opens after the failure threshold, then stops calling the provider entirely")
    void opensAndFailsFast() {
        doThrow(new NotificationDeliveryException("smtp down")).when(delegate).send(any());

        for (int i = 0; i < 4; i++) {
            assertThatThrownBy(() -> sender.send(record()))
                    .isInstanceOf(NotificationDeliveryException.class);
        }
        assertThat(circuitBreaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        // The whole point: no further provider calls once open.
        assertThatThrownBy(() -> sender.send(record()))
                .isInstanceOf(NotificationDeliveryException.class)
                .hasMessageContaining("Circuit breaker open");
        verify(delegate, times(4)).send(any());
    }

    @Test
    @DisplayName("a rejected call stays retryable so the message is redelivered, not dead-lettered")
    void rejectionIsRetryable() {
        doThrow(new NotificationDeliveryException("smtp down")).when(delegate).send(any());
        for (int i = 0; i < 4; i++) {
            assertThatThrownBy(() -> sender.send(record())).isInstanceOf(RuntimeException.class);
        }

        // NotificationDeliveryException is not in the error handler's non-retryable list, so the
        // message backs off and retries rather than dead-lettering the instant the breaker trips.
        assertThatThrownBy(() -> sender.send(record()))
                .isInstanceOf(NotificationDeliveryException.class);
    }

    @Test
    @DisplayName("non-provider exceptions do not count towards opening the breaker")
    void ignoresNonProviderFailures() {
        doThrow(new IllegalStateException("bug in our code")).when(delegate).send(any());

        for (int i = 0; i < 4; i++) {
            assertThatThrownBy(() -> sender.send(record()))
                    .isInstanceOf(IllegalStateException.class);
        }

        // A bug on our side must surface as an error, not silently trip the breaker for everyone.
        assertThat(circuitBreaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
    }

    @Test
    void exposesTheDelegateChannel() {
        assertThat(sender.channel()).isEqualTo(Channel.EMAIL);
        verify(delegate, never()).send(any());
    }
}