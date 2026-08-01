package com.common.Notification.messaging;

import com.common.Notification.domain.Channel;
import com.common.Notification.domain.SendNotificationCommand;

import java.util.Map;

/**
 * The contract for services that prefer publishing an event over calling the REST API.
 *
 * <p>This is the hardest thing in the platform to change: publishers are other teams deploying on
 * their own schedules. {@code schemaVersion} exists so a future breaking change has a safe
 * rollout — the consumer can route by version and reject what it does not understand with a clear
 * error, instead of silently mis-parsing a payload. Adding it costs nothing now and cannot be
 * retrofitted once external publishers exist.
 *
 * <p>Additive changes do not need a version bump; Jackson ignores unknown fields.
 *
 * <p>Publishers must set a stable {@code requestId} — it is the idempotency key that stops a
 * redelivered event from sending twice.
 */
public record InboundNotificationEvent(
        /** Absent is treated as v1, for publishers written before this field existed. */
        Integer schemaVersion,
        String requestId,
        String sourceService,
        Channel channel,
        String recipient,
        String templateCode,
        Map<String, Object> variables
) {

    public static final int CURRENT_SCHEMA_VERSION = 1;

    public int effectiveSchemaVersion() {
        return schemaVersion == null ? 1 : schemaVersion;
    }

    public SendNotificationCommand toCommand() {
        return new SendNotificationCommand(
                requestId, sourceService, channel, recipient, templateCode, variables);
    }
}