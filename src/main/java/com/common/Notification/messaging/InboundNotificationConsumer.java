package com.common.Notification.messaging;

import com.common.Notification.api.dto.NotificationRequest;
import com.common.Notification.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * The event-driven entry point: other services publish to {@code notification.requests} instead
 * of calling the REST API. Both paths funnel into the same {@link NotificationService#submit}.
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
        log.debug("Inbound notification request requestId={} channel={}",
                event.requestId(), event.channel());

        notificationService.submit(new NotificationRequest(
                event.requestId(),
                event.sourceService(),
                event.channel(),
                event.recipient(),
                event.templateCode(),
                event.variables()
        ));
    }
}