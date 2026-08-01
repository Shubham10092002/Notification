package com.common.Notification.messaging;

import com.common.Notification.domain.Channel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaTopicsTest {

    @ParameterizedTest
    @EnumSource(Channel.class)
    @DisplayName("every channel derives a topic name without a lookup table")
    void derivesTopicForEveryChannel(Channel channel) {
        // forChannel used to be an exhaustive switch, so each new channel meant editing it.
        assertThat(KafkaTopics.forChannel(channel))
                .isEqualTo("notification." + channel.name().toLowerCase());
    }

    @Test
    @DisplayName("the listener constants match the convention, so the two cannot drift")
    void constantsMatchConvention() {
        // @KafkaListener needs compile-time constants, so these still exist. This pins them to
        // the derived form — if they ever disagree, workers would consume a topic the producer
        // never writes to, and nothing else would catch it.
        assertThat(KafkaTopics.EMAIL).isEqualTo(KafkaTopics.forChannel(Channel.EMAIL));
        assertThat(KafkaTopics.SMS).isEqualTo(KafkaTopics.forChannel(Channel.SMS));
    }

    @Test
    void appendsDltSuffix() {
        assertThat(KafkaTopics.dltFor(KafkaTopics.EMAIL)).isEqualTo("notification.email.DLT");
        assertThat(KafkaTopics.dltFor(KafkaTopics.INBOUND_REQUESTS))
                .isEqualTo("notification.requests.DLT");
    }
}