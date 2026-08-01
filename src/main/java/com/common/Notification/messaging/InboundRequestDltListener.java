package com.common.Notification.messaging;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Last stop for inbound requests that could not be accepted — malformed JSON, an unknown
 * channel, a template that does not exist.
 *
 * <p>Unlike the channel DLTs there is no notification row to mark: acceptance never happened.
 * All that can be done is make the failure loud and keep the payload on the topic for
 * inspection, so a broken publisher is noticed rather than silently discarded.
 *
 * <p>The payload is taken as raw bytes because by definition it may not parse.
 */
@Component
@Slf4j
public class InboundRequestDltListener {

    @KafkaListener(
            topics = KafkaTopics.INBOUND_REQUESTS + KafkaTopics.DLT_SUFFIX,
            groupId = "notification-inbound-dlt"
    )
    public void onInboundDlt(@Payload(required = false) byte[] payload,
                             @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false)
                             String errorMessage,
                             @Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false)
                             String originalTopic) {
        log.error("Rejected inbound notification request from topic={} size={}B reason={}",
                originalTopic, payload == null ? 0 : payload.length,
                errorMessage == null ? "unknown" : errorMessage);
    }
}