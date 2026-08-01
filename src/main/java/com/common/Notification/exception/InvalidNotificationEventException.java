package com.common.Notification.exception;

/**
 * An inbound Kafka event failed validation.
 *
 * <p>Non-retryable by design: a payload with a missing {@code requestId} or an oversized recipient
 * will be exactly as invalid on the next delivery. Retrying only delays the DLT entry and holds up
 * the partition behind it.
 */
public class InvalidNotificationEventException extends RuntimeException {

    public InvalidNotificationEventException(String message) {
        super(message);
    }
}