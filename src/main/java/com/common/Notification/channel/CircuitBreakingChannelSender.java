package com.common.Notification.channel;

import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationRecord;
import com.common.Notification.exception.NotificationDeliveryException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Wraps a {@link ChannelSender} in a circuit breaker.
 *
 * <p>Decorating rather than annotating the senders keeps resilience policy in one place and means
 * a channel added later cannot forget to apply it — the registry wraps whatever it is given.
 *
 * <p>Each channel gets its own breaker, so an SMS provider outage cannot stop email.
 */
@RequiredArgsConstructor
@Slf4j
public class CircuitBreakingChannelSender implements ChannelSender {

    private final ChannelSender delegate;
    private final CircuitBreaker circuitBreaker;

    @Override
    public Channel channel() {
        return delegate.channel();
    }

    @Override
    public void send(NotificationRecord record) {
        try {
            circuitBreaker.executeRunnable(() -> delegate.send(record));
        } catch (CallNotPermittedException ex) {
            // Breaker is open: fail immediately instead of spending a timeout on a provider we
            // already know is down. Still a retryable exception, so the message is redelivered
            // rather than dead-lettered the moment the breaker trips.
            log.warn("Circuit open for channel={}, skipping provider call for notificationId={}",
                    channel(), record.getId());
            throw new NotificationDeliveryException(
                    "Circuit breaker open for channel " + channel(), ex);
        }
    }
}