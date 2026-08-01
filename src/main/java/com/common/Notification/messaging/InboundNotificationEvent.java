package com.common.Notification.messaging;

import com.common.Notification.domain.Channel;

import java.util.Map;

/**
 * The contract for services that prefer publishing an event over calling the REST API.
 *
 * <p>Mirrors the REST request body, so both entry points converge on the same accept path.
 * Publishers must set a stable {@code requestId} — it is the idempotency key that stops a
 * redelivered event from sending twice.
 */
public record InboundNotificationEvent(
        String requestId,
        String sourceService,
        Channel channel,
        String recipient,
        String templateCode,
        Map<String, Object> variables
) {
}