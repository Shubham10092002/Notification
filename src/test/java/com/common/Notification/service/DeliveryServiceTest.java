package com.common.Notification.service;

import com.common.Notification.channel.ChannelSender;
import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationRecord;
import com.common.Notification.domain.NotificationRepository;
import com.common.Notification.domain.NotificationStatus;
import com.common.Notification.exception.NotificationDeliveryException;
import com.common.Notification.exception.RateLimitExceededException;
import com.common.Notification.ratelimit.RedisRateLimiter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DeliveryServiceTest {

    @Mock
    private NotificationRepository notificationRepository;
    @Mock
    private RedisRateLimiter rateLimiter;
    @Mock
    private ChannelSender emailSender;

    private DeliveryService deliveryService;

    @BeforeEach
    void setUp() {
        when(emailSender.channel()).thenReturn(Channel.EMAIL);
        when(rateLimiter.tryAcquire(anyString(), anyInt(), any(Duration.class))).thenReturn(true);
        deliveryService = new DeliveryService(
                notificationRepository, rateLimiter, List.of(emailSender), new ChannelRateLimits());
    }

    private NotificationRecord record() {
        return NotificationRecord.accept("req-1", "svc", Channel.EMAIL,
                "jane@example.com", "WELCOME", "Hi", "Hello");
    }

    @Test
    @DisplayName("a successful send marks the record SENT and counts the attempt")
    void deliversSuccessfully() {
        NotificationRecord record = record();
        when(notificationRepository.findById(record.getId())).thenReturn(Optional.of(record));

        deliveryService.deliver(record.getId(), Channel.EMAIL);

        assertThat(record.getStatus()).isEqualTo(NotificationStatus.SENT);
        assertThat(record.getAttempts()).isEqualTo(1);
        assertThat(record.getSentAt()).isNotNull();
        verify(emailSender).send(record);
    }

    @Test
    @DisplayName("a redelivered message for an already-sent record does not send twice")
    void skipsAlreadyDelivered() {
        NotificationRecord record = record();
        record.markSent();
        when(notificationRepository.findById(record.getId())).thenReturn(Optional.of(record));

        deliveryService.deliver(record.getId(), Channel.EMAIL);

        verify(emailSender, never()).send(any());
    }

    @Test
    @DisplayName("hitting the rate limit throws so Kafka retries, and nothing is sent")
    void defersWhenRateLimited() {
        NotificationRecord record = record();
        when(notificationRepository.findById(record.getId())).thenReturn(Optional.of(record));
        when(rateLimiter.tryAcquire(anyString(), anyInt(), any(Duration.class))).thenReturn(false);

        assertThatThrownBy(() -> deliveryService.deliver(record.getId(), Channel.EMAIL))
                .isInstanceOf(RateLimitExceededException.class);

        verify(emailSender, never()).send(any());
        // Not counted as an attempt: being throttled is not a delivery failure.
        assertThat(record.getAttempts()).isZero();
    }

    @Test
    @DisplayName("a provider failure records the error and rethrows for the retry handler")
    void recordsFailureAndRethrows() {
        NotificationRecord record = record();
        when(notificationRepository.findById(record.getId())).thenReturn(Optional.of(record));
        doThrow(new NotificationDeliveryException("smtp down")).when(emailSender).send(record);

        assertThatThrownBy(() -> deliveryService.deliver(record.getId(), Channel.EMAIL))
                .isInstanceOf(NotificationDeliveryException.class);

        assertThat(record.getStatus()).isEqualTo(NotificationStatus.FAILED);
        assertThat(record.getLastError()).contains("smtp down");
        assertThat(record.getAttempts()).isEqualTo(1);
    }

    @Test
    @DisplayName("an event for an unknown id is swallowed rather than replayed forever")
    void ignoresUnknownNotification() {
        when(notificationRepository.findById("ghost")).thenReturn(Optional.empty());

        deliveryService.deliver("ghost", Channel.EMAIL);

        verify(emailSender, never()).send(any());
    }

    @Test
    @DisplayName("retries exhausted parks the record as DEAD_LETTER")
    void marksDeadLettered() {
        NotificationRecord record = record();
        when(notificationRepository.findById(record.getId())).thenReturn(Optional.of(record));

        deliveryService.markDeadLettered(record.getId(), "gave up");

        assertThat(record.getStatus()).isEqualTo(NotificationStatus.DEAD_LETTER);
        assertThat(record.getLastError()).isEqualTo("gave up");
    }
}