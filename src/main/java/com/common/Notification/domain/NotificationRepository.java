package com.common.Notification.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface NotificationRepository extends JpaRepository<NotificationRecord, String> {

    /** Idempotency lookup: a repeated submission resolves to the original record. */
    Optional<NotificationRecord> findByRequestId(String requestId);
}