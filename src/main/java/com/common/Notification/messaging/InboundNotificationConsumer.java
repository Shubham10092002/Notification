package com.common.Notification.messaging;

import com.common.Notification.exception.InvalidNotificationEventException;
import com.common.Notification.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * The event-driven entry point: other services publish to {@code notification.requests} instead
 * of calling the REST API. Both paths funnel into the same {@link NotificationService#submit}.
 *
 * <p>No validation logic here. The command carries the constraints and the service is
 * {@code @Validated}, so this path is checked by exactly the same rules as the REST body — the
 * two boundaries cannot drift apart, because there is only one set of rules.
 *
 * <p>What this class does own is the wire contract: rejecting a schema version it was not
 * written to understand, rather than silently mis-parsing it.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class InboundNotificationConsumer {

    private final NotificationService notificationService;

    @KafkaListener(
            topics = KafkaTopics.INBOUND_REQUESTS,
            groupId = "notification-inbound"
    )
    public void onInboundRequest(InboundNotificationEvent event) {
        if (event == null) {
            throw new InvalidNotificationEventException("Empty inbound notification event");
        }

        int version = event.effectiveSchemaVersion();
        if (version != InboundNotificationEvent.CURRENT_SCHEMA_VERSION) {
            // Fail loudly rather than parse a payload whose meaning may have changed. A publisher
            // that has moved ahead of this consumer gets a diagnosable DLT entry, not corrupt data.
            throw new InvalidNotificationEventException(
                    "Unsupported inbound event schemaVersion=" + version
                            + " (this consumer understands "
                            + InboundNotificationEvent.CURRENT_SCHEMA_VERSION + ")");
        }

        log.debug("Inbound notification request requestId={} channel={} schemaVersion={}",
                event.requestId(), event.channel(), version);

        notificationService.submit(event.toCommand());
    }
}