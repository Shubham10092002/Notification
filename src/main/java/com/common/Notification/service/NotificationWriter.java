package com.common.Notification.service;

import com.common.Notification.domain.NotificationRecord;
import com.common.Notification.domain.NotificationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Owns the insert transaction for a newly accepted notification.
 *
 * <p>Separate from {@link NotificationService} so the caller can catch a unique-constraint
 * violation <em>outside</em> the transaction. Catching it inside is useless: once the constraint
 * fires, the transaction is rollback-only and the persistence context is poisoned, so the
 * follow-up "who won the race?" query fails too.
 */
@Component
@RequiredArgsConstructor
public class NotificationWriter {

    private final NotificationRepository notificationRepository;
    private final ApplicationEventPublisher eventPublisher;

    /**
     * @throws org.springframework.dao.DataIntegrityViolationException if another thread inserted
     *         the same requestId first. Caught by the caller, outside this transaction.
     */
    @Transactional
    public NotificationRecord insert(NotificationRecord record) {
        NotificationRecord saved = notificationRepository.saveAndFlush(record);
        // Delivered after commit, never before — see NotificationAcceptedEvent.
        eventPublisher.publishEvent(new NotificationAcceptedEvent(saved.getId(), saved.getChannel()));
        return saved;
    }
}