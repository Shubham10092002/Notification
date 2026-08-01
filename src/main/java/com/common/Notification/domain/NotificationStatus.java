package com.common.Notification.domain;

/**
 * Lifecycle of a single notification.
 *
 * <pre>
 * ACCEPTED -> QUEUED -> SENT
 *                    -> FAILED -> (retried) -> SENT
 *                              -> DEAD_LETTER   (retries exhausted, landed on the DLT)
 * </pre>
 */
public enum NotificationStatus {
    /** Persisted and de-duplicated, not yet published to Kafka. */
    ACCEPTED,
    /** Published to the channel topic, awaiting a worker. */
    QUEUED,
    /** Handed to the provider successfully. */
    SENT,
    /** A delivery attempt failed; the message is still within its retry budget. */
    FAILED,
    /** Retries exhausted. The record is parked and needs manual intervention. */
    DEAD_LETTER
}