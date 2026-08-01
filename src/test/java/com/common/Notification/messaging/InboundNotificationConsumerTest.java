package com.common.Notification.messaging;

import com.common.Notification.api.dto.NotificationRequest;
import com.common.Notification.domain.Channel;
import com.common.Notification.exception.InvalidNotificationEventException;
import com.common.Notification.service.NotificationService;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Uses a real validator rather than a mock — the point of these tests is that the constraints
 * declared on {@link NotificationRequest} actually fire on the Kafka path, which a stubbed
 * validator could not demonstrate.
 */
@ExtendWith(MockitoExtension.class)
class InboundNotificationConsumerTest {

    private static ValidatorFactory validatorFactory;
    private static Validator validator;

    @Mock
    private NotificationService notificationService;

    private InboundNotificationConsumer consumer;

    @BeforeAll
    static void startValidator() {
        validatorFactory = Validation.buildDefaultValidatorFactory();
        validator = validatorFactory.getValidator();
    }

    @AfterAll
    static void stopValidator() {
        validatorFactory.close();
    }

    @BeforeEach
    void setUp() {
        consumer = new InboundNotificationConsumer(notificationService, validator);
    }

    private InboundNotificationEvent event(String requestId, Channel channel, String recipient) {
        return new InboundNotificationEvent(
                requestId, "order-service", channel, recipient, "WELCOME", Map.of("name", "Jane"));
    }

    @Test
    @DisplayName("a valid event is submitted")
    void submitsValidEvent() {
        consumer.onInboundRequest(event("req-1", Channel.EMAIL, "jane@example.com"));

        verify(notificationService).submit(any(NotificationRequest.class));
    }

    @Test
    @DisplayName("a null requestId is rejected — it would silently defeat idempotency")
    void rejectsNullRequestId() {
        assertThatThrownBy(() -> consumer.onInboundRequest(event(null, Channel.EMAIL, "jane@example.com")))
                .isInstanceOf(InvalidNotificationEventException.class)
                .hasMessageContaining("requestId");

        // Without validation this reached submit(), where findByRequestId(null) matches nothing,
        // so every redelivery inserted a new row and sent the notification again.
        verify(notificationService, never()).submit(any());
    }

    @Test
    @DisplayName("a null channel is rejected rather than failing deep in persistence")
    void rejectsNullChannel() {
        assertThatThrownBy(() -> consumer.onInboundRequest(event("req-1", null, "jane@example.com")))
                .isInstanceOf(InvalidNotificationEventException.class)
                .hasMessageContaining("channel");

        verify(notificationService, never()).submit(any());
    }

    @Test
    @DisplayName("an oversized recipient is rejected before it hits a column limit")
    void rejectsOversizedRecipient() {
        String tooLong = "a".repeat(321) + "@example.com";

        assertThatThrownBy(() -> consumer.onInboundRequest(event("req-1", Channel.EMAIL, tooLong)))
                .isInstanceOf(InvalidNotificationEventException.class)
                .hasMessageContaining("recipient");

        verify(notificationService, never()).submit(any());
    }

    @Test
    @DisplayName("the rejection message names fields but never their values")
    void rejectionDoesNotLeakRecipient() {
        assertThatThrownBy(() -> consumer.onInboundRequest(event(null, Channel.EMAIL, "jane@example.com")))
                .isInstanceOf(InvalidNotificationEventException.class)
                // Recipients are PII and must not travel into logs or DLT headers.
                .hasMessageNotContaining("jane@example.com");
    }

    @Test
    @DisplayName("a null payload is rejected rather than throwing NPE")
    void rejectsNullEvent() {
        assertThatThrownBy(() -> consumer.onInboundRequest(null))
                .isInstanceOf(InvalidNotificationEventException.class);

        verify(notificationService, never()).submit(any());
    }
}