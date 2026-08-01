package com.common.Notification.service;

import com.common.Notification.domain.Channel;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Per-channel send budgets, bound from {@code notification.rate-limit.*}.
 *
 * <p>Separate limits per channel because the constraint is the provider's quota, and an SMS
 * gateway's allowance has nothing to do with an SMTP relay's.
 */
@Component
@ConfigurationProperties(prefix = "notification.rate-limit")
public class ChannelRateLimits {

    private int emailPerMinute = 600;
    private int smsPerMinute = 120;

    public Limit forChannel(Channel channel) {
        int permits = switch (channel) {
            case EMAIL -> emailPerMinute;
            case SMS -> smsPerMinute;
        };
        return new Limit(permits, Duration.ofMinutes(1));
    }

    public int getEmailPerMinute() {
        return emailPerMinute;
    }

    public void setEmailPerMinute(int emailPerMinute) {
        this.emailPerMinute = emailPerMinute;
    }

    public int getSmsPerMinute() {
        return smsPerMinute;
    }

    public void setSmsPerMinute(int smsPerMinute) {
        this.smsPerMinute = smsPerMinute;
    }

    public record Limit(int permits, Duration window) {
    }
}