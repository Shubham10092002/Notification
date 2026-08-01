package com.common.Notification.service;

import com.common.Notification.channel.ChannelSender;
import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationRecord;
import com.common.Notification.domain.NotificationRepository;
import com.common.Notification.exception.RateLimitExceededException;
import com.common.Notification.ratelimit.RedisRateLimiter;
import com.common.Notification.support.Redaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * The delivery half of the pipeline, shared by every channel worker.
 *
 * <p>Each worker is a thin Kafka listener; the ordering that actually matters — idempotency
 * check, rate limit, send, status update — lives here once so the channels cannot drift apart.
 *
 * <p>Deliberately <strong>not</strong> {@code @Transactional}. The provider call is a network
 * round trip that can block for seconds; holding a pooled database connection across it would
 * exhaust the pool long before the provider became the bottleneck. State transitions are
 * delegated to {@link NotificationStateWriter}, which commits each one in its own short
 * transaction — so a failure recorded here survives the exception this method rethrows.
 */
@Service
@Slf4j
public class DeliveryService {

    private final NotificationRepository notificationRepository;
    private final NotificationStateWriter stateWriter;
    private final RedisRateLimiter rateLimiter;
    private final Map<Channel, ChannelSender> senders = new EnumMap<>(Channel.class);
    private final ChannelRateLimits rateLimits;

    public DeliveryService(NotificationRepository notificationRepository,
                           NotificationStateWriter stateWriter,
                           RedisRateLimiter rateLimiter,
                           List<ChannelSender> channelSenders,
                           ChannelRateLimits rateLimits) {
        this.notificationRepository = notificationRepository;
        this.stateWriter = stateWriter;
        this.rateLimiter = rateLimiter;
        this.rateLimits = rateLimits;
        for (ChannelSender sender : channelSenders) {
            this.senders.put(sender.channel(), sender);
        }
    }

    /**
     * Delivers one notification.
     *
     * <p>Throws on retryable failures so the Kafka error handler owns backoff and dead-lettering
     * — retry logic is deliberately not duplicated here.
     */
    public void deliver(String notificationId, Channel channel) {
        NotificationRecord record = notificationRepository.findById(notificationId).orElse(null);
        if (record == null) {
            // Nothing to retry against; swallow so the message is not replayed forever.
            log.error("Received event for unknown notificationId={} on channel={}",
                    notificationId, channel);
            return;
        }

        if (record.isAlreadyDelivered()) {
            // Kafka is at-least-once, so duplicates are expected rather than exceptional.
            log.debug("Skipping already-delivered notificationId={}", notificationId);
            return;
        }

        ChannelSender sender = senders.get(channel);
        if (sender == null) {
            // Non-retryable: no amount of waiting will register a sender for this channel.
            stateWriter.markDeadLettered(notificationId, "No sender registered for channel " + channel);
            throw new IllegalStateException("No ChannelSender for channel " + channel);
        }

        ChannelRateLimits.Limit limit = rateLimits.forChannel(channel);
        if (!rateLimiter.tryAcquire("channel:" + channel, limit.permits(), limit.window())) {
            log.warn("Rate limit hit for channel={}, deferring notificationId={}",
                    channel, notificationId);
            // Not counted as an attempt: being throttled is not a delivery failure.
            throw new RateLimitExceededException(
                    "Rate limit exceeded for channel " + channel + ", will retry");
        }

        try {
            sender.send(record);
        } catch (RuntimeException ex) {
            stateWriter.markFailed(notificationId, ex.getMessage());
            throw ex;
        }

        stateWriter.markSent(notificationId);
        log.info("Delivered notificationId={} channel={} recipient={}",
                notificationId, channel, Redaction.mask(record.getRecipient()));
    }

    /**
     * Terminal handler once retries are exhausted. Parks the record as DEAD_LETTER so it shows
     * up in queries instead of quietly disappearing onto a topic nobody reads.
     */
    public void markDeadLettered(String notificationId, String error) {
        stateWriter.markDeadLettered(notificationId, error);
        log.error("Dead-lettered notificationId={}: {}", notificationId, error);
    }
}