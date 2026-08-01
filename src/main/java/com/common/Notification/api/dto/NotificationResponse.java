package com.common.Notification.api.dto;

import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationRecord;
import com.common.Notification.domain.NotificationStatus;
import com.common.Notification.support.Redaction;

import java.time.Instant;

/**
 * Status view of a notification.
 *
 * <p>The recipient is returned masked. Callers already know who they addressed, and an
 * unmasked value here would leak PII into logs, traces and dashboards downstream.
 */
public record NotificationResponse(
        String notificationId,
        String requestId,
        Channel channel,
        String recipient,
        NotificationStatus status,
        int attempts,
        String lastError,
        Instant createdAt,
        Instant sentAt
) {

    public static NotificationResponse from(NotificationRecord record) {
        return new NotificationResponse(
                record.getId(),
                record.getRequestId(),
                record.getChannel(),
                Redaction.mask(record.getRecipient()),
                record.getStatus(),
                record.getAttempts(),
                record.getLastError(),
                record.getCreatedAt(),
                record.getSentAt()
        );
    }
}