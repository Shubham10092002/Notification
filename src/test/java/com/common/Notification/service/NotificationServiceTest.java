package com.common.Notification.service;

import com.common.Notification.api.dto.NotificationRequest;
import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationRecord;
import com.common.Notification.domain.NotificationRepository;
import com.common.Notification.domain.NotificationStatus;
import com.common.Notification.messaging.NotificationEventProducer;
import com.common.Notification.template.TemplateService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationServiceTest {

    @Mock
    private NotificationRepository notificationRepository;
    @Mock
    private TemplateService templateService;
    @Mock
    private NotificationEventProducer eventProducer;

    @InjectMocks
    private NotificationService notificationService;

    private NotificationRequest request(Channel channel) {
        return new NotificationRequest(
                "req-1", "payment-service", channel,
                channel == Channel.SMS ? "+919876543210" : "jane@example.com",
                "WELCOME", Map.of("name", "Jane"));
    }

    @Test
    @DisplayName("a repeat requestId returns the original record and does not re-publish")
    void isSubmissionIdempotent() {
        NotificationRecord existing = NotificationRecord.accept(
                "req-1", "payment-service", Channel.EMAIL, "jane@example.com",
                "WELCOME", "Hi", "Hello Jane");
        when(notificationRepository.findByRequestId("req-1")).thenReturn(Optional.of(existing));

        NotificationRecord result = notificationService.submit(request(Channel.EMAIL));

        assertThat(result).isSameAs(existing);
        // The critical assertion: a duplicate must not put a second event on the topic.
        verify(eventProducer, never()).publish(any());
        verify(notificationRepository, never()).saveAndFlush(any());
    }

    @Test
    @DisplayName("a new request is persisted, queued and published")
    void submitsNewNotification() {
        when(notificationRepository.findByRequestId("req-1")).thenReturn(Optional.empty());
        when(templateService.render("WELCOME", Channel.EMAIL, Map.of("name", "Jane")))
                .thenReturn(new TemplateService.Rendered("Welcome Jane", "Hello Jane"));
        when(notificationRepository.saveAndFlush(any(NotificationRecord.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        NotificationRecord result = notificationService.submit(request(Channel.EMAIL));

        assertThat(result.getStatus()).isEqualTo(NotificationStatus.QUEUED);
        assertThat(result.getSubject()).isEqualTo("Welcome Jane");
        assertThat(result.getBody()).isEqualTo("Hello Jane");
        verify(eventProducer).publish(result);
    }

    @Test
    @DisplayName("SMS carries no subject even when the template defines one")
    void dropsSubjectForSms() {
        when(notificationRepository.findByRequestId("req-1")).thenReturn(Optional.empty());
        when(templateService.render("WELCOME", Channel.SMS, Map.of("name", "Jane")))
                .thenReturn(new TemplateService.Rendered("ignored subject", "Hello Jane"));
        when(notificationRepository.saveAndFlush(any(NotificationRecord.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        NotificationRecord result = notificationService.submit(request(Channel.SMS));

        assertThat(result.getSubject()).isNull();
        assertThat(result.getBody()).isEqualTo("Hello Jane");
    }
}