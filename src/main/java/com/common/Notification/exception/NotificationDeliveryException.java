package com.common.Notification.exception;

/**
 * A delivery attempt failed in a way that may succeed later (provider timeout, 5xx, throttling).
 * Retryable — the Kafka error handler will back off and try again before dead-lettering.
 */
public class NotificationDeliveryException extends RuntimeException {

    public NotificationDeliveryException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotificationDeliveryException(String message) {
        super(message);
    }
}