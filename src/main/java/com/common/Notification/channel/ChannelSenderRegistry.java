package com.common.Notification.channel;

import com.common.Notification.domain.Channel;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves the sender for a channel, with a per-channel circuit breaker already applied.
 *
 * <p>Wrapping happens here rather than in {@code DeliveryService} so that delivery orchestration
 * stays free of resilience concerns, and rather than in each sender so that no channel can be
 * added without protection.
 */
@Component
@Slf4j
public class ChannelSenderRegistry {

    private final Map<Channel, ChannelSender> senders;

    public ChannelSenderRegistry(List<ChannelSender> channelSenders,
                                 CircuitBreakerRegistry circuitBreakerRegistry) {
        Map<Channel, ChannelSender> resolved = new EnumMap<>(Channel.class);
        for (ChannelSender sender : channelSenders) {
            Channel channel = sender.channel();
            CircuitBreaker breaker = circuitBreakerRegistry.circuitBreaker("channel-" + channel);
            breaker.getEventPublisher().onStateTransition(event ->
                    log.warn("Circuit breaker for channel={} moved {} -> {}", channel,
                            event.getStateTransition().getFromState(),
                            event.getStateTransition().getToState()));
            resolved.put(channel, new CircuitBreakingChannelSender(sender, breaker));
        }
        // Fixed at construction and never mutated, so it is safe to share across consumer threads.
        this.senders = Collections.unmodifiableMap(resolved);
    }

    /** @return null when no sender is registered for the channel. */
    public ChannelSender senderFor(Channel channel) {
        return senders.get(channel);
    }
}