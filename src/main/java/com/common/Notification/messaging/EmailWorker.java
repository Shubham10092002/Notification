package com.common.Notification.messaging;

import com.common.Notification.domain.Channel;
import com.common.Notification.service.DeliveryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Email Worker from the design doc. Its own consumer group, so it scales and lags
 * independently of SMS.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class EmailWorker {

    private final DeliveryService deliveryService;

    @KafkaListener(
            topics = KafkaTopics.EMAIL,
            groupId = "notification-email-worker"
    )
    public void onEmailEvent(NotificationEvent event) {
        deliveryService.deliver(event.notificationId(), Channel.EMAIL);
    }

    /**
     * Retries exhausted. Records the terminal state; the message stays on the DLT for replay
     * once whatever broke has been fixed.
     */
    @KafkaListener(
            topics = KafkaTopics.EMAIL + KafkaTopics.DLT_SUFFIX,
            groupId = "notification-email-dlt"
    )
    public void onEmailDlt(NotificationEvent event,
                           @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false)
                           String errorMessage) {
        deliveryService.markDeadLettered(event.notificationId(),
                errorMessage == null ? "Retries exhausted" : errorMessage);
    }
}