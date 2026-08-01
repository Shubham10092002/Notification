package com.common.Notification.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * A reusable message template, owned by the Template Service in the design doc.
 *
 * <p>Bodies use {@code {{placeholder}}} syntax; see
 * {@link com.common.Notification.template.TemplateService} for rendering.
 *
 * <p>Not cached directly: {@code TemplateView} is what goes into Redis, so this stays a plain
 * persistence type.
 */
@Entity
@Table(
        name = "notification_template",
        indexes = @Index(name = "ux_template_code_channel", columnList = "code,channel", unique = true)
)
@Getter
@Setter
@NoArgsConstructor
public class NotificationTemplate {

    @Id
    @Column(name = "id", length = 36, nullable = false, updatable = false)
    private String id;

    @Column(name = "code", length = 64, nullable = false)
    private String code;

    @Enumerated(EnumType.STRING)
    @Column(name = "channel", length = 16, nullable = false)
    private Channel channel;

    /** Ignored for SMS, which has no subject line. */
    @Column(name = "subject", length = 512)
    private String subject;

    @Lob
    @Column(name = "body", nullable = false)
    private String body;

    @Column(name = "active", nullable = false)
    private boolean active = true;
}