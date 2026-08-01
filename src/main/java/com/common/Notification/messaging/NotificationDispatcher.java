package com.common.Notification.messaging;

import com.common.Notification.domain.Channel;
import com.common.Notification.service.NotificationAcceptedEvent;
import com.common.Notification.service.NotificationStateWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.concurrent.Executor;

/**
 * Publishes accepted notifications to Kafka once — and only once — the database transaction has
 * committed.
 *
 * <p>This is the platform's answer to the dual-write problem the design doc calls out as the
 * Outbox Pattern. The {@code notification} table <em>is</em> the outbox:
 *
 * <ul>
 *   <li>the row is committed first, in state {@code ACCEPTED};</li>
 *   <li>the event is published after commit, and only a confirmed broker ack promotes the row
 *       to {@code QUEUED};</li>
 *   <li>anything left in {@code ACCEPTED} is a publish that never landed, and
 *       {@link StuckNotificationSweeper} retries it.</li>
 * </ul>
 *
 * <p>So a broker outage during accept degrades to a delayed send rather than a silently lost
 * notification, without needing a separate outbox table.
 */
@Component
@Slf4j
public class NotificationDispatcher {

    private final NotificationEventProducer eventProducer;
    private final NotificationStateWriter stateWriter;
    private final Executor callbackExecutor;

    public NotificationDispatcher(NotificationEventProducer eventProducer,
                                  NotificationStateWriter stateWriter,
                                  @Qualifier("kafkaCallbackExecutor") Executor callbackExecutor) {
        this.eventProducer = eventProducer;
        this.stateWriter = stateWriter;
        this.callbackExecutor = callbackExecutor;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onNotificationAccepted(NotificationAcceptedEvent event) {
        dispatch(event.notificationId(), event.channel());
    }

    /** Also used by the sweeper when replaying a notification whose publish never landed. */
    public void dispatch(String notificationId, Channel channel) {
        eventProducer.publish(notificationId, channel)
                // whenCompleteAsync, not whenComplete: the callback writes to the database and
                // must not run on the producer's I/O thread.
                .whenCompleteAsync((result, throwable) -> {
                    if (throwable != null) {
                        // Leave the row in ACCEPTED so the sweeper retries it. Do not fail the
                        // caller: the notification is durable, only its dispatch is delayed.
                        log.error("Failed to publish notificationId={} to {}; left ACCEPTED for retry",
                                notificationId, KafkaTopics.forChannel(channel), throwable);
                        return;
                    }
                    stateWriter.markQueued(notificationId);
                }, callbackExecutor);
    }
}