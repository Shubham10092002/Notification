package com.common.Notification.api;

import com.common.Notification.api.dto.NotificationRequest;
import com.common.Notification.api.dto.NotificationResponse;
import com.common.Notification.domain.NotificationRecord;
import com.common.Notification.service.NotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The REST entry point for services that would rather make an HTTP call than produce to Kafka.
 */
@RestController
@RequestMapping("/api/v1/notifications")
@RequiredArgsConstructor
public class NotificationController {

    private final NotificationService notificationService;

    /**
     * Accepts a notification for delivery.
     *
     * <p>Returns 202, not 200 — delivery is asynchronous, and the response says only that the
     * request was durably accepted and queued. Poll the GET endpoint for the outcome.
     */
    @PostMapping
    public ResponseEntity<NotificationResponse> submit(@RequestBody NotificationRequest request) {
        // Validation happens on the command inside the service, so both entry points share it.
        NotificationRecord record = notificationService.submit(request.toCommand());
        return ResponseEntity.accepted().body(NotificationResponse.from(record));
    }

    /**
     * Replays a dead-lettered notification after the underlying problem is fixed.
     *
     * @return 204 on success, 409 if the notification is not in DEAD_LETTER, 404 if unknown.
     */
    @PostMapping("/{notificationId}/replay")
    public ResponseEntity<Void> replay(@PathVariable String notificationId) {
        if (notificationService.findById(notificationId).isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
        return notificationService.replay(notificationId)
                ? ResponseEntity.noContent().build()
                : ResponseEntity.status(HttpStatus.CONFLICT).build();
    }

    @GetMapping("/{notificationId}")
    public ResponseEntity<NotificationResponse> getById(@PathVariable String notificationId) {
        return notificationService.findById(notificationId)
                .map(NotificationResponse::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).build());
    }

    /** Lookup by the caller's own idempotency key, for callers that did not keep our id. */
    @GetMapping
    public ResponseEntity<NotificationResponse> getByRequestId(@RequestParam String requestId) {
        return notificationService.findByRequestId(requestId)
                .map(NotificationResponse::from)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).build());
    }
}