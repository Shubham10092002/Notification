package com.common.Notification.messaging;

import com.common.Notification.domain.Channel;
import com.common.Notification.service.DeliveryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/** SMS Worker from the design doc. */
@Component
@RequiredArgsConstructor
@Slf4j
public class SmsWorker {

    private final DeliveryService deliveryService;

    @KafkaListener(
            topics = KafkaTopics.SMS,
            groupId = "notification-sms-worker"
    )
    public void onSmsEvent(NotificationEvent event) {
        deliveryService.deliver(event.notificationId(), Channel.SMS);
    }

    @KafkaListener(
            topics = KafkaTopics.SMS + KafkaTopics.DLT_SUFFIX,
            groupId = "notification-sms-dlt"
    )
    public void onSmsDlt(NotificationEvent event,
                         @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false)
                         String errorMessage) {
        deliveryService.markDeadLettered(event.notificationId(),
                errorMessage == null ? "Retries exhausted" : errorMessage);
    }
}