package com.common.Notification.service;

import com.common.Notification.channel.ChannelSender;
import com.common.Notification.channel.ChannelSenderRegistry;
import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationRecord;
import com.common.Notification.domain.NotificationRepository;
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
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DeliveryServiceTest {

    @Mock
    private NotificationRepository notificationRepository;
    @Mock
    private NotificationStateWriter stateWriter;
    @Mock
    private RedisRateLimiter rateLimiter;
    @Mock
    private ChannelSender emailSender;
    @Mock
    private ChannelSenderRegistry senderRegistry;

    private DeliveryService deliveryService;

    @BeforeEach
    void setUp() {
        when(senderRegistry.senderFor(Channel.EMAIL)).thenReturn(emailSender);
        when(senderRegistry.senderFor(Channel.SMS)).thenReturn(null);
        when(rateLimiter.tryAcquire(anyString(), anyInt(), any(Duration.class))).thenReturn(true);
        deliveryService = new DeliveryService(
                notificationRepository, stateWriter, rateLimiter,
                senderRegistry, new ChannelRateLimits());
    }

    private NotificationRecord record() {
        NotificationRecord record = NotificationRecord.accept("req-1", "svc", Channel.EMAIL,
                "jane@example.com", "WELCOME", "Hi", "Hello");
        when(notificationRepository.findById(record.getId())).thenReturn(Optional.of(record));
        return record;
    }

    @Test
    @DisplayName("a successful send is recorded as SENT")
    void deliversSuccessfully() {
        NotificationRecord record = record();

        deliveryService.deliver(record.getId(), Channel.EMAIL);

        verify(emailSender).send(record);
        verify(stateWriter).markSent(record.getId());
    }

    @Test
    @DisplayName("a redelivered message for an already-sent record does not send twice")
    void skipsAlreadyDelivered() {
        NotificationRecord record = record();
        record.markSent();

        deliveryService.deliver(record.getId(), Channel.EMAIL);

        verify(emailSender, never()).send(any());
        verifyNoInteractions(stateWriter);
    }

    @Test
    @DisplayName("hitting the rate limit throws so Kafka retries, and nothing is sent or recorded")
    void defersWhenRateLimited() {
        NotificationRecord record = record();
        when(rateLimiter.tryAcquire(anyString(), anyInt(), any(Duration.class))).thenReturn(false);

        assertThatThrownBy(() -> deliveryService.deliver(record.getId(), Channel.EMAIL))
                .isInstanceOf(RateLimitExceededException.class);

        verify(emailSender, never()).send(any());
        // Throttling is not a delivery attempt, so it must not burn the retry budget.
        verifyNoInteractions(stateWriter);
    }

    @Test
    @DisplayName("a provider failure is PERSISTED before the exception propagates")
    void recordsFailureBeforeRethrowing() {
        NotificationRecord record = record();
        doThrow(new NotificationDeliveryException("smtp down")).when(emailSender).send(record);

        assertThatThrownBy(() -> deliveryService.deliver(record.getId(), Channel.EMAIL))
                .isInstanceOf(NotificationDeliveryException.class);

        // Regression guard: this used to be written inside the same transaction the rethrow
        // rolled back, so attempts stayed at 0 and lastError was silently lost.
        verify(stateWriter).markFailed(eq(record.getId()), contains("smtp down"));
        verify(stateWriter, never()).markSent(anyString());
    }

    @Test
    @DisplayName("an event for an unknown id is swallowed rather than replayed forever")
    void ignoresUnknownNotification() {
        when(notificationRepository.findById("ghost")).thenReturn(Optional.empty());

        deliveryService.deliver("ghost", Channel.EMAIL);

        verify(emailSender, never()).send(any());
        verifyNoInteractions(stateWriter);
    }

    @Test
    @DisplayName("an unroutable channel is dead-lettered immediately, not retried")
    void deadLettersUnknownChannel() {
        NotificationRecord record = NotificationRecord.accept("req-2", "svc", Channel.SMS,
                "+919876543210", "WELCOME", null, "Hello");
        when(notificationRepository.findById(record.getId())).thenReturn(Optional.of(record));

        assertThatThrownBy(() -> deliveryService.deliver(record.getId(), Channel.SMS))
                .isInstanceOf(IllegalStateException.class);

        verify(stateWriter).markDeadLettered(eq(record.getId()), contains("No sender"));
    }

    @Test
    @DisplayName("retries exhausted parks the record as DEAD_LETTER")
    void marksDeadLettered() {
        deliveryService.markDeadLettered("abc", "gave up");

        verify(stateWriter).markDeadLettered("abc", "gave up");
    }
}