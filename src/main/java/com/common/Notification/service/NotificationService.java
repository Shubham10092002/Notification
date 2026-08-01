package com.common.Notification.service;

import com.common.Notification.api.dto.NotificationRequest;
import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationRecord;
import com.common.Notification.domain.NotificationRepository;
import com.common.Notification.messaging.NotificationDispatcher;
import com.common.Notification.support.Redaction;
import com.common.Notification.template.TemplateService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * The accept half of the pipeline. Both entry points — REST and the inbound Kafka topic —
 * land here, so idempotency and template rendering behave identically regardless of how the
 * request arrived.
 *
 * <p>Rendering happens at accept time, not delivery time: a bad template then fails fast with
 * a 4xx to the caller instead of surfacing later as a mystery dead-lettered message.
 *
 * <p>Not {@code @Transactional} itself. The insert is delegated to {@link NotificationWriter}
 * so a duplicate-key collision can be handled after that transaction has rolled back.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationService {

    private final NotificationRepository notificationRepository;
    private final NotificationWriter notificationWriter;
    private final TemplateService templateService;
    private final NotificationStateWriter stateWriter;
    private final NotificationDispatcher dispatcher;

    public NotificationRecord submit(NotificationRequest request) {
        // Fast path: the overwhelmingly common duplicate is a caller retry, not a race.
        Optional<NotificationRecord> existing =
                notificationRepository.findByRequestId(request.requestId());
        if (existing.isPresent()) {
            log.info("Duplicate submission requestId={}, returning existing notificationId={}",
                    request.requestId(), existing.get().getId());
            return existing.get();
        }

        Channel channel = request.channel();
        TemplateService.Rendered rendered =
                templateService.render(request.templateCode(), channel, request.variables());

        NotificationRecord record = NotificationRecord.accept(
                request.requestId(),
                request.sourceService(),
                channel,
                request.recipient(),
                request.templateCode(),
                channel == Channel.SMS ? null : rendered.subject(),
                rendered.body()
        );

        try {
            NotificationRecord saved = notificationWriter.insert(record);
            log.info("Accepted notificationId={} channel={} recipient={} template={}",
                    saved.getId(), channel, Redaction.mask(saved.getRecipient()),
                    request.templateCode());
            return saved;
        } catch (DataIntegrityViolationException ex) {
            // Slow path: two submissions of the same requestId raced and the unique index
            // settled it. Safe to re-query here because the failed transaction has ended.
            log.info("Concurrent duplicate for requestId={}, returning the winning record",
                    request.requestId());
            return notificationRepository.findByRequestId(request.requestId())
                    .orElseThrow(() -> ex);
        }
    }

    /**
     * Puts a dead-lettered notification back on its channel topic.
     *
     * <p>Retry backoff is intentionally short, so a provider outage longer than a couple of
     * minutes dead-letters rather than blocking the partition. This is the operational way back
     * from that: fix the provider, then replay. Safe to call repeatedly — delivery is idempotent.
     *
     * @return false if the notification does not exist or is not in DEAD_LETTER.
     */
    public boolean replay(String notificationId) {
        Optional<NotificationRecord> record = notificationRepository.findById(notificationId);
        if (record.isEmpty() || !stateWriter.resetForReplay(notificationId)) {
            return false;
        }
        dispatcher.dispatch(notificationId, record.get().getChannel());
        log.info("Replayed dead-lettered notificationId={}", notificationId);
        return true;
    }

    @Transactional(readOnly = true)
    public Optional<NotificationRecord> findById(String notificationId) {
        return notificationRepository.findById(notificationId);
    }

    @Transactional(readOnly = true)
    public Optional<NotificationRecord> findByRequestId(String requestId) {
        return notificationRepository.findByRequestId(requestId);
    }
}