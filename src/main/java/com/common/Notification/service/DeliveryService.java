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
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * The delivery half of the pipeline, shared by every channel worker.
 *
 * <p>Each worker is a thin Kafka listener; the ordering that actually matters — idempotency
 * check, rate limit, send, status update — lives here once so the channels cannot drift apart.
 */
@Service
@Slf4j
public class DeliveryService {

    private final NotificationRepository notificationRepository;
    private final RedisRateLimiter rateLimiter;
    private final Map<Channel, ChannelSender> senders = new EnumMap<>(Channel.class);
    private final ChannelRateLimits rateLimits;

    public DeliveryService(NotificationRepository notificationRepository,
                           RedisRateLimiter rateLimiter,
                           List<ChannelSender> channelSenders,
                           ChannelRateLimits rateLimits) {
        this.notificationRepository = notificationRepository;
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
    @Transactional
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
            record.markDeadLettered("No sender registered for channel " + channel);
            notificationRepository.save(record);
            throw new IllegalStateException("No ChannelSender for channel " + channel);
        }

        ChannelRateLimits.Limit limit = rateLimits.forChannel(channel);
        if (!rateLimiter.tryAcquire("channel:" + channel, limit.permits(), limit.window())) {
            log.warn("Rate limit hit for channel={}, deferring notificationId={}",
                    channel, notificationId);
            throw new RateLimitExceededException(
                    "Rate limit exceeded for channel " + channel + ", will retry");
        }

        record.recordAttempt();
        try {
            sender.send(record);
            record.markSent();
            notificationRepository.save(record);
            log.info("Delivered notificationId={} channel={} recipient={} attempts={}",
                    notificationId, channel, Redaction.mask(record.getRecipient()),
                    record.getAttempts());
        } catch (RuntimeException ex) {
            record.markFailed(ex.getMessage());
            notificationRepository.save(record);
            throw ex;
        }
    }

    /**
     * Terminal handler once retries are exhausted. Parks the record as DEAD_LETTER so it shows
     * up in queries instead of quietly disappearing onto a topic nobody reads.
     */
    @Transactional
    public void markDeadLettered(String notificationId, String error) {
        notificationRepository.findById(notificationId).ifPresent(record -> {
            record.markDeadLettered(error);
            notificationRepository.save(record);
            log.error("Dead-lettered notificationId={} after {} attempts: {}",
                    notificationId, record.getAttempts(), error);
        });
    }
}