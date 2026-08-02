package com.common.Notification.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface NotificationRepository extends JpaRepository<NotificationRecord, String> {

    /** Idempotency lookup: a repeated submission resolves to the original record. */
    Optional<NotificationRecord> findByRequestId(String requestId);

    /**
     * Notifications that were committed but whose Kafka publish never landed.
     *
     * <p>Bounded and oldest-first so a long broker outage drains in fair order without loading
     * an unbounded backlog into memory.
     *
     * <p>Returns a projection, so Spring Data generates {@code select id, channel} rather than
     * selecting whole rows. The sweeper only needs those two fields, and full entities would drag
     * every {@code @Lob} body and recipient into memory — tens of megabytes per sweep, discarded
     * immediately, at exactly the moment the system is already degraded.
     */
    List<NotificationDispatchView> findTop200ByStatusAndCreatedAtBeforeOrderByCreatedAtAsc(
            NotificationStatus status, Instant createdBefore);
}