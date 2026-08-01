package com.common.Notification.domain;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.Map;

/**
 * The platform's internal contract for "send this notification".
 *
 * <p>Both entry points map to this, so the HTTP body and the Kafka event schema can evolve
 * independently of the business logic and of each other. Before this existed the service and
 * messaging layers imported the web DTO directly — a dependency pointing inward from a delivery
 * mechanism to the core, which meant a Jackson annotation added for JSON shaping changed a type
 * the business logic depended on.
 *
 * <p>Constraints live here rather than on the DTOs, so both boundaries enforce identical rules by
 * construction instead of by discipline.
 *
 * @param requestId     caller-owned idempotency key; resubmitting returns the original
 *                      notification rather than sending again
 * @param sourceService which service asked, for auditing
 * @param channel       EMAIL or SMS
 * @param recipient     email address or phone number — PII, never logged unmasked
 * @param templateCode  which template to render
 * @param variables     values substituted into the template's {{placeholders}}
 */
public record SendNotificationCommand(

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