package com.common.Notification.messaging;

import com.common.Notification.domain.NotificationRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Publishes to the channel topic, keyed by notification id so all events for one
     * notification land on the same partition and are processed in order.
     */
    public void publish(NotificationRecord record) {
        String topic = KafkaTopics.forChannel(record.getChannel());
        NotificationEvent event = new NotificationEvent(record.getId(), record.getChannel());
        kafkaTemplate.send(topic, record.getId(), event);
        log.debug("Published notificationId={} to topic={}", record.getId(), topic);
    }
}