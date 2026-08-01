package com.common.Notification.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Lob;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

/**
 * The system of record for one notification, from acceptance through delivery.
 *
 * <p>This row is what makes delivery auditable and idempotent: workers re-read it before
 * sending, so a redelivered Kafka message cannot send the same notification twice.
 */
@Entity
@Table(
        name = "notification",
        indexes = {
                @Index(name = "ux_notification_request_id", columnList = "request_id", unique = true),
                @Index(name = "ix_notification_status", columnList = "status"),
                @Index(name = "ix_notification_created_at", columnList = "created_at")
        }
)
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PUBLIC)
public class NotificationRecord {

    @Id
    @Column(name = "id", length = 36, nullable = false, updatable = false)
    private String id;

    /**
     * Caller-supplied idempotency key. Unique, so a retried submission collapses onto the
     * original record instead of producing a duplicate send.
     */
    @Column(name = "request_id", length = 128, nullable = false, updatable = false)
    private String requestId;

    /** Originating service, for auditing and per-source rate limiting. */
    @Column(name = "source_service", length = 64)
    private String sourceService;

    @Enumerated(EnumType.STRING)
    @Column(name = "channel", length = 16, nullable = false, updatable = false)
    private Channel channel;

    /** Email address or phone number. Treated as PII — never logged in the clear. */
    @Column(name = "recipient", length = 320, nullable = false)
    private String recipient;

    @Column(name = "template_code", length = 64)
    private String templateCode;

    @Column(name = "subject", length = 512)
    private String subject;

    @Lob
    @Column(name = "body")
    private String body;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", length = 16, nullable = false)
    private NotificationStatus status;

    @Column(name = "attempts", nullable = false)
    private int attempts;

    @Column(name = "last_error", length = 1024)
    private String lastError;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "sent_at")
    private Instant sentAt;

    /**
     * Prevents lost updates when two workers touch the same record after a partition rebalance.
     *
     * <p>It does <em>not</em> prevent a duplicate send: the provider call happens before the
     * status write, so both workers may have already sent. Genuine send-once would need the
     * provider's own idempotency key. What stops duplicates in practice is the
     * {@code isAlreadyDelivered()} check plus keying events by notification id, so redeliveries
     * land on the same partition and are processed in order.
     */
    @Version
    @Column(name = "version")
    private Long version;

    public static NotificationRecord accept(String requestId,
                                            String sourceService,
                                            Channel channel,
                                            String recipient,
                                            String templateCode,
                                            String subject,
                                            String body) {
        NotificationRecord record = new NotificationRecord();
        record.id = UUID.randomUUID().toString();
        record.requestId = requestId;
        record.sourceService = sourceService;
        record.channel = channel;
        record.recipient = recipient;
        record.templateCode = templateCode;
        record.subject = subject;
        record.body = body;
        record.status = NotificationStatus.ACCEPTED;
        record.attempts = 0;
        record.createdAt = Instant.now();
        record.updatedAt = record.createdAt;
        return record;
    }

    public void markQueued() {
        this.status = NotificationStatus.QUEUED;
    }

    public void markSent() {
        this.status = NotificationStatus.SENT;
        this.sentAt = Instant.now();
        this.lastError = null;
    }

    public void markFailed(String error) {
        this.status = NotificationStatus.FAILED;
        this.lastError = truncateError(error);
    }

    public void markDeadLettered(String error) {
        this.status = NotificationStatus.DEAD_LETTER;
        this.lastError = truncateError(error);
    }

    public void recordAttempt() {
        this.attempts++;
    }

    /** Returns a dead-lettered notification to the start of the pipeline for a manual replay. */
    public void resetToAccepted() {
        this.status = NotificationStatus.ACCEPTED;
        this.lastError = null;
    }

    public boolean isAlreadyDelivered() {
        return this.status == NotificationStatus.SENT;
    }

    @PreUpdate
    void onUpdate() {
        this.updatedAt = Instant.now();
    }

    /** last_error is a bounded column; provider stack traces routinely overflow it. */
    private static String truncateError(String error) {
        if (error == null) {
            return null;
        }
        return error.length() <= 1024 ? error : error.substring(0, 1024);
    }
}