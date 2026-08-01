package com.common.Notification.api.dto;

import com.common.Notification.domain.Channel;
import com.common.Notification.domain.SendNotificationCommand;

import java.util.Map;

/**
 * The HTTP request body.
 *
 * <p>Deliberately carries no constraints. It exists only to shape JSON and map to
 * {@link SendNotificationCommand}, which owns validation and is checked by method validation on
 * the service. That leaves the HTTP contract free to change — field names, Jackson annotations,
 * deprecated aliases — without touching the business layer, and guarantees the REST and Kafka
 * entry points cannot drift into enforcing different rules.
 *
 * @param requestId    caller-owned idempotency key; resubmitting returns the original notification
 * @param sourceService which service asked, for auditing
 * @param channel      EMAIL or SMS
 * @param recipient    email address or phone number
 * @param templateCode which template to render
 * @param variables    values substituted into the template's {{placeholders}}
 */
public record NotificationRequest(
        String requestId,
        String sourceService,
        Channel channel,
        String recipient,
        String templateCode,
        Map<String, Object> variables
) {

    public SendNotificationCommand toCommand() {
        return new SendNotificationCommand(
                requestId, sourceService, channel, recipient, templateCode, variables);
    }
}