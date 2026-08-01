package com.common.Notification.exception;

/**
 * The channel's send budget for the current window is spent.
 *
 * <p>Retryable on purpose: the worker throws this so the message is backed off and redelivered
 * once the window rolls, rather than being dropped or dead-lettered.
 */
public class RateLimitExceededException extends RuntimeException {

    public RateLimitExceededException(String message) {
        super(message);
    }
}