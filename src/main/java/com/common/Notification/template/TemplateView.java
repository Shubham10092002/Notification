package com.common.Notification.template;

import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationTemplate;

/**
 * Immutable, persistence-free view of a template — this is what goes into Redis.
 *
 * <p>Caching the JPA entity instead caused three problems. The cached object came back detached,
 * so any lazy association would throw on a cache hit but work on a miss — behaviour that differs
 * by cache state is miserable to debug. The Redis payload was coupled to the persistence schema,
 * so adding a column could break deserialization of entries written by the previous version
 * during a rolling deploy. And a detached entity handed to callers can be mutated and
 * accidentally re-attached, writing cache-shaped data back to the database.
 *
 * <p>A record has none of those properties, and is trivially thread safe.
 */
public record TemplateView(String code, Channel channel, String subject, String body) {

    static TemplateView from(NotificationTemplate entity) {
        return new TemplateView(entity.getCode(), entity.getChannel(),
                entity.getSubject(), entity.getBody());
    }
}