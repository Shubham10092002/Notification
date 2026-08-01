package com.common.Notification.messaging;

import com.common.Notification.api.dto.NotificationRequest;
import com.common.Notification.exception.InvalidNotificationEventException;
import com.common.Notification.service.NotificationService;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * The event-driven entry point: other services publish to {@code notification.requests} instead
 * of calling the REST API. Both paths funnel into the same {@link NotificationService#submit}.
 *
 * <p>Validation is applied explicitly here. The REST boundary gets it from {@code @Valid} on the
 * controller, and without the equivalent on this side the two entry points would enforce
 * different contracts — most damagingly, an event with a null {@code requestId} would defeat
 * idempotency entirely, because {@code findByRequestId(null)} matches nothing and every
 * redelivery would send again.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class InboundNotificationConsumer {

    private final NotificationService notificationService;
    private final Validator validator;

    @KafkaListener(
            topics = KafkaTopics.INBOUND_REQUESTS,
            groupId = "notification-inbound"
    )
    public void onInboundRequest(InboundNotificationEvent event) {
        if (event == null) {
            throw new InvalidNotificationEventException("Empty inbound notification event");
        }

        log.debug("Inbound notification request requestId={} channel={}",
                event.requestId(), event.channel());

        NotificationRequest request = new NotificationRequest(
                event.requestId(),
                event.sourceService(),
                event.channel(),
                event.recipient(),
                event.templateCode(),
                event.variables()
        );

        Set<ConstraintViolation<NotificationRequest>> violations = validator.validate(request);
        if (!violations.isEmpty()) {
            // Field names and messages only — never the offending values, which include the
            // recipient.
            String detail = violations.stream()
                    .map(violation -> violation.getPropertyPath() + " " + violation.getMessage())
                    .sorted()
                    .collect(Collectors.joining("; "));
            throw new InvalidNotificationEventException("Invalid inbound event: " + detail);
        }

        notificationService.submit(request);
    }
}