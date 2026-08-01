package com.common.Notification.messaging;

import com.common.Notification.domain.Channel;

import java.util.Locale;

/**
 * Topic names.
 *
 * <p>Per-channel topics rather than one shared topic: email and SMS have very different
 * throughput and provider limits, and a backlog of one must not stall the other. Each gets its
 * own consumer group and can be scaled independently.
 *
 * <p>Channel topics follow a convention rather than a lookup table. {@code forChannel} used to be
 * an exhaustive {@code switch}, which meant every new channel required editing this class — one
 * of several places a channel had to be registered. The convention removes that; the constants
 * remain only because {@code @KafkaListener} needs compile-time constants, and a test pins them
 * to the convention so the two cannot drift.
 *
 * <p>{@code .DLT} suffixes are the Spring Kafka convention and are produced automatically by
 * the dead-letter recoverer once retries are exhausted.
 */
public final class KafkaTopics {

    /** Inbound topic other services publish to instead of calling the REST API. */
    public static final String INBOUND_REQUESTS = "notification.requests";

    public static final String CHANNEL_TOPIC_PREFIX = "notification.";

    public static final String EMAIL = "notification.email";
    public static final String SMS = "notification.sms";

    public static final String DLT_SUFFIX = ".DLT";

    private KafkaTopics() {
    }

    public static String forChannel(Channel channel) {
        return CHANNEL_TOPIC_PREFIX + channel.name().toLowerCase(Locale.ROOT);
    }

    public static String dltFor(String topic) {
        return topic + DLT_SUFFIX;
    }
}