package com.common.Notification.domain;

/**
 * Projection carrying only what dispatch needs.
 *
 * <p>The sweeper used to load full entities to read two fields. Each carried a {@code @Lob} body
 * and a recipient, so a 200-row sweep could pull 10–20 MB of HTML email bodies into memory every
 * 30 seconds and discard all of it — and it did that precisely when the system was already
 * degraded, since that is when the backlog exists. It also dragged recipient PII into memory for
 * no reason.
 */
public interface NotificationDispatchView {

    String getId();

    Channel getChannel();
}