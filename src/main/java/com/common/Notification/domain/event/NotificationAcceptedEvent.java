package com.common.Notification.domain.event;

import com.common.Notification.domain.Channel;

/**
 * Raised inside the accept transaction, consumed only after it commits.
 *
 * <p>This is what keeps the database and Kafka from disagreeing: publishing inside the
 * transaction risks putting an event on the topic for a row that then rolls back, leaving a
 * worker chasing a notification that does not exist.
 *
 * <p>Lives in {@code domain.event} rather than in {@code service} so that both the service and
 * messaging packages can depend on it without depending on each other — that mutual dependency
 * was a package cycle, and it is the thing that would block ever splitting this into separate
 * deployables.
 */
public record NotificationAcceptedEvent(String notificationId, Channel channel) {
}