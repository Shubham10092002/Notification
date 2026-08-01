package com.common.Notification.messaging;

import com.common.Notification.domain.Channel;

/**
 * Topic names.
 *
 * <p>Per-channel topics rather than one shared topic: email and SMS have very different
 * throughput and provider limits, and a backlog of one must not stall the other. Each gets its
 * own consumer group and can be scaled independently.
 *
 * <p>{@code .DLT} suffixes are the Spring Kafka convention and are produced automatically by
 * the dead-letter recoverer once retries are exhausted.
 */
public final class KafkaTopics {

    /** Inbound topic other services publish to instead of calling the REST API. */
    public static final String INBOUND_REQUESTS = "notification.requests";

    public static final String EMAIL = "notification.email";
    public static final String SMS = "notification.sms";

    public static final String DLT_SUFFIX = ".DLT";

    private KafkaTopics() {
    }

    public static String forChannel(Channel channel) {
        return switch (channel) {
            case EMAIL -> EMAIL;
            case SMS -> SMS;
        };
    }
}