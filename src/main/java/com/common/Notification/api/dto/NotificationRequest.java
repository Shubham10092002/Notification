package com.common.Notification.api.dto;

import com.common.Notification.domain.Channel;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.Map;

/**
 * What a calling service sends, over REST or Kafka.
 *
 * @param requestId    caller-owned idempotency key; resubmitting the same id returns the
 *                     original notification instead of sending again
 * @param sourceService which service asked, for auditing
 * @param channel      EMAIL or SMS
 * @param recipient    email address or phone number
 * @param templateCode which template to render
 * @param variables    values substituted into the template's {{placeholders}}
 */
public record NotificationRequest(

        @NotBlank(message = "requestId is required and is the idempotency key")
        @Size(max = 128)
        String requestId,

        @Size(max = 64)
        String sourceService,

        @NotNull(message = "channel is required (EMAIL or SMS)")
        Channel channel,

        @NotBlank(message = "recipient is required")
        @Size(max = 320)
        String recipient,

        @NotBlank(message = "templateCode is required")
        @Size(max = 64)
        String templateCode,

        Map<String, Object> variables
) {
}