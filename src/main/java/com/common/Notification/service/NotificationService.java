package com.common.Notification.service;

import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationRecord;
import com.common.Notification.domain.NotificationRepository;
import com.common.Notification.domain.SendNotificationCommand;
import com.common.Notification.domain.event.NotificationAcceptedEvent;
import com.common.Notification.support.Redaction;
import com.common.Notification.template.TemplateService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.validation.annotation.Validated;

import java.util.Optional;

/**
 * The accept half of the pipeline. Both entry points — REST and the inbound Kafka topic — land
 * here, so idempotency, validation and template rendering behave identically regardless of how
 * the request arrived.
 *
 * <p>{@code @Validated} plus {@code @Valid} on the parameter means the command is checked by
 * method validation on every call. Putting it here rather than on the controller is what makes
 * the guarantee real: the Kafka path gets the same constraints without anyone remembering to
 * repeat them.
 *
 * <p>Rendering happens at accept time, not delivery time: a bad template then fails fast with
 * a 4xx to the caller instead of surfacing later as a mystery dead-lettered message.
 *
 * <p>Not {@code @Transactional} itself. The insert is delegated to {@link NotificationWriter}
 * so a duplicate-key collision can be handled after that transaction has rolled back.
 *
 * <p>Dispatch happens by publishing {@link NotificationAcceptedEvent}, never by calling the
 * messaging layer directly — that keeps the dependency arrow one-way and the package graph
 * acyclic.
 */
@Service
@Validated
@RequiredArgsConstructor
@Slf4j
public class NotificationService {

    private final NotificationRepository notificationRepository;
    private final NotificationWriter notificationWriter;
    private final TemplateService templateService;
    private final NotificationStateWriter stateWriter;
    private final ApplicationEventPublisher eventPublisher;

    public NotificationRecord submit(@Valid SendNotificationCommand command) {
        // Fast path: the overwhelmingly common duplicate is a caller retry, not a race.
        Optional<NotificationRecord> existing =
                notificationRepository.findByRequestId(command.requestId());
        if (existing.isPresent()) {
            log.info("Duplicate submission requestId={}, returning existing notificationId={}",
                    command.requestId(), existing.get().getId());
            return existing.get();
        }

        Channel channel = command.channel();
        TemplateService.Rendered rendered =
                templateService.render(command.templateCode(), channel, command.variables());

        NotificationRecord record = NotificationRecord.accept(
                command.requestId(),
                command.sourceService(),
                channel,
                command.recipient(),
                command.templateCode(),
                channel == Channel.SMS ? null : rendered.subject(),
                rendered.body()
        );

        try {
            NotificationRecord saved = notificationWriter.insert(record);
            log.info("Accepted notificationId={} channel={} recipient={} template={}",
                    saved.getId(), channel, Redaction.mask(saved.getRecipient()),
                    command.templateCode());
            return saved;
        } catch (DataIntegrityViolationException ex) {
            // Slow path: two submissions of the same requestId raced and the unique index
            // settled it. Safe to re-query here because the failed transaction has ended.
            log.info("Concurrent duplicate for requestId={}, returning the winning record",
                    command.requestId());
            return notificationRepository.findByRequestId(command.requestId())
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
        // Same route as a fresh acceptance: one dispatch path, and no dependency on messaging.
        eventPublisher.publishEvent(
                new NotificationAcceptedEvent(notificationId, record.get().getChannel()));
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