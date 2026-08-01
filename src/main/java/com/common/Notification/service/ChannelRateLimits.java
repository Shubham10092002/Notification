package com.common.Notification.service;

import com.common.Notification.domain.Channel;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;

/**
 * Per-channel send budgets, bound from {@code notification.rate-limit.channels.*}.
 *
 * <p>Separate limits per channel because the constraint is the provider's quota, and an SMS
 * gateway's allowance has nothing to do with an SMTP relay's.
 *
 * <p>Map-keyed rather than a field-and-{@code switch} per channel: adding a channel should be a
 * properties entry, not a code change. A channel with no entry falls back to a deliberately
 * conservative default rather than to zero, because a zero limit silently blocks the channel
 * entirely — the worst possible outcome of a missing config key.
 */
@Component
@ConfigurationProperties(prefix = "notification.rate-limit")
public class ChannelRateLimits {

    private Map<Channel, Limit> channels = new EnumMap<>(Channel.class);
    private Limit defaultLimit = new Limit(120, Duration.ofMinutes(1));

    public Limit forChannel(Channel channel) {
        return channels.getOrDefault(channel, defaultLimit);
    }

    public Map<Channel, Limit> getChannels() {
        return channels;
    }

    public void setChannels(Map<Channel, Limit> channels) {
        this.channels = channels;
    }

    public Limit getDefaultLimit() {
        return defaultLimit;
    }

    public void setDefaultLimit(Limit defaultLimit) {
        this.defaultLimit = defaultLimit;
    }

    /** Mutable for configuration binding; read-only in practice once the context is built. */
    public static class Limit {

        private int permits;
        private Duration window = Duration.ofMinutes(1);

        public Limit() {
        }

        public Limit(int permits, Duration window) {
            this.permits = permits;
            this.window = window;
        }

        public int permits() {
            return permits;
        }

        public Duration window() {
            return window;
        }

        public int getPermits() {
            return permits;
        }

        public void setPermits(int permits) {
            this.permits = permits;
        }

        public Duration getWindow() {
            return window;
        }

        public void setWindow(Duration window) {
            this.window = window;
        }
    }
}