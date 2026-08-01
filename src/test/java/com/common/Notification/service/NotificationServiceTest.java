package com.common.Notification.service;

import com.common.Notification.api.dto.NotificationRequest;
import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationRecord;
import com.common.Notification.domain.NotificationRepository;
import com.common.Notification.domain.NotificationStatus;
import com.common.Notification.messaging.NotificationDispatcher;
import com.common.Notification.template.TemplateService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationServiceTest {

    @Mock
    private NotificationRepository notificationRepository;
    @Mock
    private NotificationWriter notificationWriter;
    @Mock
    private TemplateService templateService;
    @Mock
    private NotificationStateWriter stateWriter;
    @Mock
    private NotificationDispatcher dispatcher;

    @InjectMocks
    private NotificationService notificationService;

    private NotificationRequest request(Channel channel) {
        return new NotificationRequest(
                "req-1", "payment-service", channel,
                channel == Channel.SMS ? "+919876543210" : "jane@example.com",
                "WELCOME", Map.of("name", "Jane"));
    }

    private NotificationRecord record(Channel channel) {
        return NotificationRecord.accept("req-1", "payment-service", channel,
                "jane@example.com", "WELCOME", "Hi", "Hello Jane");
    }

    @Test
    @DisplayName("a repeat requestId returns the original record and does not insert again")
    void isSubmissionIdempotent() {
        NotificationRecord existing = record(Channel.EMAIL);
        when(notificationRepository.findByRequestId("req-1")).thenReturn(Optional.of(existing));

        NotificationRecord result = notificationService.submit(request(Channel.EMAIL));

        assertThat(result).isSameAs(existing);
        verify(notificationWriter, never()).insert(any());
    }

    @Test
    @DisplayName("a new request is rendered, inserted and left ACCEPTED until the broker acks")
    void submitsNewNotification() {
        when(notificationRepository.findByRequestId("req-1")).thenReturn(Optional.empty());
        when(templateService.render("WELCOME", Channel.EMAIL, Map.of("name", "Jane")))
                .thenReturn(new TemplateService.Rendered("Welcome Jane", "Hello Jane"));
        when(notificationWriter.insert(any(NotificationRecord.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        NotificationRecord result = notificationService.submit(request(Channel.EMAIL));

        // ACCEPTED, not QUEUED: only a confirmed Kafka ack promotes it, which is what lets the
        // sweeper tell "never published" apart from "published and in flight".
        assertThat(result.getStatus()).isEqualTo(NotificationStatus.ACCEPTED);
        assertThat(result.getSubject()).isEqualTo("Welcome Jane");
        assertThat(result.getBody()).isEqualTo("Hello Jane");
    }

    @Test
    @DisplayName("SMS carries no subject even when the template defines one")
    void dropsSubjectForSms() {
        when(notificationRepository.findByRequestId("req-1")).thenReturn(Optional.empty());
        when(templateService.render("WELCOME", Channel.SMS, Map.of("name", "Jane")))
                .thenReturn(new TemplateService.Rendered("ignored subject", "Hello Jane"));
        when(notificationWriter.insert(any(NotificationRecord.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        NotificationRecord result = notificationService.submit(request(Channel.SMS));

        assertThat(result.getSubject()).isNull();
        assertThat(result.getBody()).isEqualTo("Hello Jane");
    }

    @Test
    @DisplayName("a concurrent duplicate resolves to the winning row instead of failing the caller")
    void resolvesConcurrentDuplicate() {
        NotificationRecord winner = record(Channel.EMAIL);
        when(notificationRepository.findByRequestId("req-1"))
                .thenReturn(Optional.empty())      // fast-path miss
                .thenReturn(Optional.of(winner));  // re-query after the constraint fired
        when(templateService.render("WELCOME", Channel.EMAIL, Map.of("name", "Jane")))
                .thenReturn(new TemplateService.Rendered("Welcome Jane", "Hello Jane"));
        when(notificationWriter.insert(any(NotificationRecord.class)))
                .thenThrow(new DataIntegrityViolationException("duplicate requestId"));

        NotificationRecord result = notificationService.submit(request(Channel.EMAIL));

        // The re-query only works because the failed insert ran in its own transaction, which
        // has already ended by the time we get here.
        assertThat(result).isSameAs(winner);
    }

    @Test
    @DisplayName("a duplicate-key error with no winning row surfaces rather than being swallowed")
    void rethrowsWhenDuplicateCannotBeResolved() {
        when(notificationRepository.findByRequestId("req-1")).thenReturn(Optional.empty());
        when(templateService.render("WELCOME", Channel.EMAIL, Map.of("name", "Jane")))
                .thenReturn(new TemplateService.Rendered("Welcome Jane", "Hello Jane"));
        when(notificationWriter.insert(any(NotificationRecord.class)))
                .thenThrow(new DataIntegrityViolationException("some other constraint"));

        assertThatThrownBy(() -> notificationService.submit(request(Channel.EMAIL)))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @Test
    @DisplayName("replay only republishes a record the writer agreed to reset")
    void replayRespectsTerminalState() {
        NotificationRecord record = record(Channel.EMAIL);
        when(notificationRepository.findById(record.getId())).thenReturn(Optional.of(record));
        when(stateWriter.resetForReplay(record.getId())).thenReturn(false);

        assertThat(notificationService.replay(record.getId())).isFalse();
        verify(dispatcher, never()).dispatch(any(), any());
    }

    @Test
    @DisplayName("replay of a dead-lettered record puts it back on its channel topic")
    void replayRepublishes() {
        NotificationRecord record = record(Channel.EMAIL);
        when(notificationRepository.findById(record.getId())).thenReturn(Optional.of(record));
        when(stateWriter.resetForReplay(record.getId())).thenReturn(true);

        assertThat(notificationService.replay(record.getId())).isTrue();
        verify(dispatcher).dispatch(record.getId(), Channel.EMAIL);
    }
}