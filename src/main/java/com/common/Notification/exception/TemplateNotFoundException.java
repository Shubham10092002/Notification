package com.common.Notification.exception;

import com.common.Notification.domain.Channel;

/** Non-retryable: a missing template will still be missing on the next attempt. */
public class TemplateNotFoundException extends RuntimeException {

    public TemplateNotFoundException(String code, Channel channel) {
        super("No active template '" + code + "' for channel " + channel);
    }
}