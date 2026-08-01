package com.common.Notification.messaging;

import com.common.Notification.domain.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@RequiredArgsConstructor
@Slf4j
public class NotificationEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Publishes to the channel topic, keyed by notification id so all events for one
     * notification land on the same partition and are processed in order.
     *
     * <p>Returns the future rather than firing and forgetting. {@code send} is asynchronous: if
     * the broker is unreachable the call still returns normally, and without inspecting the
     * result a notification would sit in the database looking healthy while nothing was ever
     * queued. The caller is expected to attach a completion handler.
     */
    public CompletableFuture<SendResult<String, Object>> publish(String notificationId, Channel channel) {
        String topic = KafkaTopics.forChannel(channel);
        NotificationEvent event = new NotificationEvent(notificationId, channel);
        log.debug("Publishing notificationId={} to topic={}", notificationId, topic);
        return kafkaTemplate.send(topic, notificationId, event);
    }
}