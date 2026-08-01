package com.common.Notification.messaging;

import com.common.Notification.domain.Channel;

/**
 * The message on the channel topics.
 *
 * <p>Deliberately thin — it carries the notification id, not the rendered body or the
 * recipient. The worker re-reads the row, which keeps PII off the broker, keeps messages
 * small, and means a worker always acts on current state rather than a stale snapshot.
 */
public record NotificationEvent(String notificationId, Channel channel) {
}