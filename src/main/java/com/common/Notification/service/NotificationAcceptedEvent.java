package com.common.Notification.service;

import com.common.Notification.domain.Channel;

/**
 * Raised inside the accept transaction, consumed only after it commits.
 *
 * <p>This is what keeps the database and Kafka from disagreeing: publishing inside the
 * transaction risks putting an event on the topic for a row that then rolls back, leaving a
 * worker chasing a notification that does not exist.
 */
public record NotificationAcceptedEvent(String notificationId, Channel channel) {
}