package com.common.Notification;

import com.common.Notification.domain.Channel;
import com.common.Notification.service.ChannelRateLimits;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class NotificationApplicationTests {

	@Autowired
	private ChannelRateLimits rateLimits;

	@Test
	void contextLoads() {
	}

	@Test
	@DisplayName("per-channel rate limits bind from application.properties")
	void rateLimitsBindFromConfiguration() {
		// Guards a silent failure mode: @ConfigurationProperties ignores unknown keys, so when
		// this moved from email-per-minute/sms-per-minute to a channel-keyed map, stale keys
		// would have bound nothing and every channel would have quietly dropped to the default.
		assertThat(rateLimits.forChannel(Channel.EMAIL).permits()).isEqualTo(600);
		assertThat(rateLimits.forChannel(Channel.EMAIL).window()).isEqualTo(Duration.ofMinutes(1));
		assertThat(rateLimits.forChannel(Channel.SMS).permits()).isEqualTo(120);
	}

	@Test
	@DisplayName("a channel with no configured limit falls back rather than blocking")
	void unconfiguredChannelFallsBackToDefault() {
		ChannelRateLimits empty = new ChannelRateLimits();

		// Zero would silently stop the channel entirely — the worst outcome of a missing key.
		assertThat(empty.forChannel(Channel.EMAIL).permits()).isPositive();
	}
}