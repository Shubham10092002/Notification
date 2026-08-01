package com.common.Notification.channel;

import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationRecord;

/**
 * One implementation per channel. Adding PUSH or WHATSAPP later means adding an implementation
 * and a worker — nothing in the accept/persist/publish path changes.
 */
public interface ChannelSender {

    Channel channel();

    /**
     * Hands the notification to the provider.
     *
     * @throws com.common.Notification.exception.NotificationDeliveryException on a failure that
     *         may succeed on retry. Anything non-retryable should throw a different unchecked
     *         exception so the Kafka error handler dead-letters it immediately.
     */
    void send(NotificationRecord record);
}