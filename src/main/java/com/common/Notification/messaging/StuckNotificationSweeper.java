package com.common.Notification.messaging;

import com.common.Notification.domain.NotificationRecord;
import com.common.Notification.domain.NotificationRepository;
import com.common.Notification.domain.NotificationStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Recovers notifications whose Kafka publish never landed.
 *
 * <p>A row stuck in {@code ACCEPTED} means the accept transaction committed but the broker
 * never acknowledged the event — the classic outcome of a broker outage or a crash in the
 * window between commit and publish. Without this sweep those notifications would be durable
 * and permanently invisible, which is the worst possible failure mode for a notification
 * platform: the caller was told "accepted" and nothing ever arrives.
 *
 * <p>Republishing is safe because delivery is idempotent — workers skip records already marked
 * {@code SENT}, and the event is keyed by notification id.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class StuckNotificationSweeper {

    private final NotificationRepository notificationRepository;
    private final NotificationDispatcher dispatcher;

    @Value("${notification.sweeper.stuck-after-seconds:60}")
    private long stuckAfterSeconds;

    /**
     * Batched rather than unbounded: a long outage can strand a large backlog, and loading all
     * of it into one transaction would trade a delivery problem for a memory problem.
     */
    @Scheduled(
            initialDelayString = "${notification.sweeper.initial-delay-ms:30000}",
            fixedDelayString = "${notification.sweeper.interval-ms:30000}")
    @Transactional(readOnly = true)
    public void republishStuckNotifications() {
        Instant cutoff = Instant.now().minus(stuckAfterSeconds, ChronoUnit.SECONDS);
        List<NotificationRecord> stuck = notificationRepository
                .findTop200ByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
                        NotificationStatus.ACCEPTED, cutoff);

        if (stuck.isEmpty()) {
            return;
        }

        log.warn("Found {} notification(s) stuck in ACCEPTED past {}s; republishing",
                stuck.size(), stuckAfterSeconds);
        for (NotificationRecord record : stuck) {
            dispatcher.dispatch(record.getId(), record.getChannel());
        }
    }
}