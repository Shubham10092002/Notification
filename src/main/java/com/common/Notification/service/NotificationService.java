package com.common.Notification.service;

import com.common.Notification.api.dto.NotificationRequest;
import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationRecord;
import com.common.Notification.domain.NotificationRepository;
import com.common.Notification.messaging.NotificationEventProducer;
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
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationService {

    private final NotificationRepository notificationRepository;
    private final TemplateService templateService;
    private final NotificationEventProducer eventProducer;

    @Transactional
    public NotificationRecord submit(NotificationRequest request) {
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
            record = notificationRepository.saveAndFlush(record);
        } catch (DataIntegrityViolationException ex) {
            // Two concurrent submissions of the same requestId; the unique index settled it.
            // Return whichever row won rather than failing the caller.
            log.info("Concurrent duplicate for requestId={}, returning the winning record",
                    request.requestId());
            return notificationRepository.findByRequestId(request.requestId())
                    .orElseThrow(() -> ex);
        }

        record.markQueued();
        eventProducer.publish(record);

        log.info("Accepted notificationId={} channel={} recipient={} template={}",
                record.getId(), channel, Redaction.mask(record.getRecipient()),
                request.templateCode());
        return record;
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