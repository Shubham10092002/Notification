package com.common.Notification.messaging;

import com.common.Notification.domain.Channel;
import com.common.Notification.domain.SendNotificationCommand;
import com.common.Notification.exception.InvalidNotificationEventException;
import com.common.Notification.service.NotificationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * The consumer owns the wire contract only. Field-level validation is enforced by method
 * validation on {@code NotificationService}, so it is covered by
 * {@link com.common.Notification.domain.SendNotificationCommandValidationTest} rather than here.
 */
@ExtendWith(MockitoExtension.class)
class InboundNotificationConsumerTest {

    @Mock
    private NotificationService notificationService;

    private InboundNotificationConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new InboundNotificationConsumer(notificationService);
    }

    private InboundNotificationEvent event(Integer schemaVersion) {
        return new InboundNotificationEvent(schemaVersion, "req-1", "order-service",
                Channel.EMAIL, "jane@example.com", "WELCOME", Map.of("name", "Jane"));
    }

    @Test
    @DisplayName("a current-version event is mapped to a command and submitted")
    void submitsCurrentVersion() {
        consumer.onInboundRequest(event(InboundNotificationEvent.CURRENT_SCHEMA_VERSION));

        ArgumentCaptor<SendNotificationCommand> captor =
                ArgumentCaptor.forClass(SendNotificationCommand.class);
        verify(notificationService).submit(captor.capture());
        assertThat(captor.getValue().requestId()).isEqualTo("req-1");
        assertThat(captor.getValue().channel()).isEqualTo(Channel.EMAIL);
    }

    @Test
    @DisplayName("an absent schemaVersion is treated as v1, for publishers predating the field")
    void treatsMissingVersionAsV1() {
        consumer.onInboundRequest(event(null));

        verify(notificationService).submit(any(SendNotificationCommand.class));
    }

    @Test
    @DisplayName("a future schemaVersion is rejected rather than mis-parsed")
    void rejectsUnknownSchemaVersion() {
        assertThatThrownBy(() -> consumer.onInboundRequest(event(2)))
                .isInstanceOf(InvalidNotificationEventException.class)
                .hasMessageContaining("schemaVersion=2");

        // Better a diagnosable DLT entry than silently reading a payload whose meaning changed.
        verify(notificationService, never()).submit(any());
    }

    @Test
    @DisplayName("a null payload is rejected rather than throwing NPE")
    void rejectsNullEvent() {
        assertThatThrownBy(() -> consumer.onInboundRequest(null))
                .isInstanceOf(InvalidNotificationEventException.class);

        verify(notificationService, never()).submit(any());
    }

    @Test
    @DisplayName("the rejection message never contains the recipient")
    void rejectionDoesNotLeakRecipient() {
        assertThatThrownBy(() -> consumer.onInboundRequest(event(99)))
                .isInstanceOf(InvalidNotificationEventException.class)
                .hasMessageNotContaining("jane@example.com");
    }
}