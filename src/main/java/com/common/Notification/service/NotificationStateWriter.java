package com.common.Notification.service;

import com.common.Notification.domain.NotificationRecord;
import com.common.Notification.domain.NotificationRepository;
import com.common.Notification.domain.NotificationStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.function.Consumer;

/**
 * Persists notification state transitions, each in its own short transaction.
 *
 * <p>This exists to fix a specific class of bug: if a delivery failure is recorded inside the
 * same transaction that then rethrows to trigger a Kafka retry, Spring marks that transaction
 * rollback-only and the failure record is <em>discarded</em> — attempts stay at zero and
 * {@code lastError} is never written, so operators see nothing.
 *
 * <p>{@code REQUIRES_NEW} guarantees each transition commits independently of whatever the
 * caller does afterwards. Transitions are also short and hold no network calls, so a slow SMTP
 * provider can never pin a database connection.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationStateWriter {

    private final NotificationRepository notificationRepository;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markQueued(String notificationId) {
        mutate(notificationId, NotificationRecord::markQueued);
    }

    /**
     * Counts the attempt and marks it delivered in one write — attempts must move in both the
     * success and failure paths, or the retry count in the DLT is meaningless.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markSent(String notificationId) {
        mutate(notificationId, record -> {
            record.recordAttempt();
            record.markSent();
        });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markFailed(String notificationId, String error) {
        mutate(notificationId, record -> {
            record.recordAttempt();
            record.markFailed(error);
        });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markDeadLettered(String notificationId, String error) {
        mutate(notificationId, record -> record.markDeadLettered(error));
    }

    /**
     * Puts a dead-lettered notification back into the pipeline. Clears the terminal state so the
     * normal accept-side republish path can pick it up again.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean resetForReplay(String notificationId) {
        NotificationRecord record = notificationRepository.findById(notificationId).orElse(null);
        if (record == null || record.getStatus() != NotificationStatus.DEAD_LETTER) {
            return false;
        }
        record.resetToAccepted();
        notificationRepository.save(record);
        return true;
    }

    private void mutate(String notificationId, Consumer<NotificationRecord> change) {
        notificationRepository.findById(notificationId).ifPresentOrElse(
                record -> {
                    change.accept(record);
                    notificationRepository.save(record);
                },
                () -> log.error("No notification found for id={} while updating state", notificationId));
    }
}