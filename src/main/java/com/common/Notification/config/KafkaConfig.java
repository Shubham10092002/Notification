package com.common.Notification.config;

import com.common.Notification.exception.TemplateNotFoundException;
import com.common.Notification.messaging.KafkaTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Wires the doc's Retry and DLQ concepts.
 *
 * <p>Failed deliveries are retried with exponential backoff and, once the budget is exhausted,
 * published to {@code <topic>.DLT} instead of being retried forever or dropped.
 *
 * <p>No custom listener container factory is declared: Boot's auto-configured factory picks up
 * the {@link DefaultErrorHandler} bean below, so the listeners get retry/DLT behaviour while
 * still inheriting every {@code spring.kafka.*} setting.
 */
@Configuration
public class KafkaConfig {

    /**
     * Topics are created explicitly rather than relying on broker auto-creation, which defaults
     * to one partition and would cap worker parallelism at one.
     */
    @Bean
    NewTopic inboundRequestsTopic(@Value("${notification.kafka.partitions:3}") int partitions) {
        return TopicBuilder.name(KafkaTopics.INBOUND_REQUESTS).partitions(partitions).replicas(1).build();
    }

    @Bean
    NewTopic emailTopic(@Value("${notification.kafka.partitions:3}") int partitions) {
        return TopicBuilder.name(KafkaTopics.EMAIL).partitions(partitions).replicas(1).build();
    }

    @Bean
    NewTopic smsTopic(@Value("${notification.kafka.partitions:3}") int partitions) {
        return TopicBuilder.name(KafkaTopics.SMS).partitions(partitions).replicas(1).build();
    }

    @Bean
    NewTopic emailDltTopic() {
        return TopicBuilder.name(KafkaTopics.EMAIL + KafkaTopics.DLT_SUFFIX).partitions(1).replicas(1).build();
    }

    @Bean
    NewTopic smsDltTopic() {
        return TopicBuilder.name(KafkaTopics.SMS + KafkaTopics.DLT_SUFFIX).partitions(1).replicas(1).build();
    }

    @Bean
    DefaultErrorHandler notificationErrorHandler(
            KafkaOperations<?, ?> kafkaOperations,
            @Value("${notification.retry.initial-interval-ms:1000}") long initialInterval,
            @Value("${notification.retry.multiplier:2.0}") double multiplier,
            @Value("${notification.retry.max-interval-ms:60000}") long maxInterval,
            @Value("${notification.retry.max-elapsed-time-ms:900000}") long maxElapsedTime) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaOperations,
                // Single-partition DLT: volume is low and ordering there is irrelevant.
                (record, ex) -> new TopicPartition(record.topic() + KafkaTopics.DLT_SUFFIX, 0));

        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(initialInterval);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxInterval);
        backOff.setMaxElapsedTime(maxElapsedTime);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // A missing template cannot fix itself; retrying just delays the inevitable DLT entry.
        errorHandler.addNotRetryableExceptions(TemplateNotFoundException.class);

        return errorHandler;
    }
}