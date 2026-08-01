package com.common.Notification.config;

import com.common.Notification.exception.InvalidNotificationEventException;
import com.common.Notification.exception.TemplateNotFoundException;
import com.common.Notification.channel.ChannelSender;
import com.common.Notification.messaging.KafkaTopics;
import jakarta.validation.ConstraintViolationException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.support.converter.ByteArrayJacksonJsonMessageConverter;
import org.springframework.kafka.support.mapping.JacksonJavaTypeMapper;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.ArrayList;
import java.util.List;

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
     *
     * <p>Channel topics are derived from the registered {@link ChannelSender} beans, so adding a
     * channel does not mean remembering to add two more topic beans here.
     *
     * <p>Replication is configuration-driven. It used to be hardcoded to 1, which would have
     * created single-replica topics on a production cluster — one broker failure away from losing
     * every unconsumed notification, on a platform whose whole purpose is not losing them. With
     * {@code acks=all} already set, replicas=3 and minInSync=2 is what makes that setting mean
     * something.
     */
    @Bean
    KafkaAdmin.NewTopics notificationTopics(
            List<ChannelSender> channelSenders,
            @Value("${notification.kafka.partitions:3}") int partitions,
            @Value("${notification.kafka.replicas:1}") int replicas,
            @Value("${notification.kafka.min-insync-replicas:1}") int minInSync) {

        List<NewTopic> topics = new ArrayList<>();
        topics.add(topic(KafkaTopics.INBOUND_REQUESTS, partitions, replicas, minInSync));
        topics.add(topic(KafkaTopics.dltFor(KafkaTopics.INBOUND_REQUESTS), 1, replicas, minInSync));

        for (ChannelSender sender : channelSenders) {
            String channelTopic = KafkaTopics.forChannel(sender.channel());
            topics.add(topic(channelTopic, partitions, replicas, minInSync));
            // Single-partition DLT: volume is low and ordering there is irrelevant.
            topics.add(topic(KafkaTopics.dltFor(channelTopic), 1, replicas, minInSync));
        }

        return new KafkaAdmin.NewTopics(topics.toArray(NewTopic[]::new));
    }

    private static NewTopic topic(String name, int partitions, int replicas, int minInSync) {
        return TopicBuilder.name(name)
                .partitions(partitions)
                .replicas(replicas)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(minInSync))
                .build();
    }

    /**
     * Deserializes each record using the listener method's parameter type instead of a type
     * header, so services written in any language can publish plain JSON to the inbound topic.
     */
    @Bean
    ByteArrayJacksonJsonMessageConverter kafkaMessageConverter() {
        ByteArrayJacksonJsonMessageConverter converter = new ByteArrayJacksonJsonMessageConverter();
        converter.getTypeMapper().setTypePrecedence(JacksonJavaTypeMapper.TypePrecedence.INFERRED);
        return converter;
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
                (record, ex) -> new TopicPartition(KafkaTopics.dltFor(record.topic()), 0));

        ExponentialBackOff backOff = new ExponentialBackOff();
        backOff.setInitialInterval(initialInterval);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxInterval);
        backOff.setMaxElapsedTime(maxElapsedTime);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // Neither of these can fix itself on redelivery; retrying only delays the DLT entry and
        // blocks the partition behind it.
        errorHandler.addNotRetryableExceptions(
                TemplateNotFoundException.class,
                InvalidNotificationEventException.class,
                // Method validation on the service. A payload that fails constraints will fail
                // them identically on redelivery.
                ConstraintViolationException.class);

        return errorHandler;
    }
}