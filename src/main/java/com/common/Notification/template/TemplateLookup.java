package com.common.Notification.template;

import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationTemplate;
import com.common.Notification.domain.TemplateRepository;
import com.common.Notification.exception.TemplateNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

/**
 * Cached template lookups.
 *
 * <p>This lives in its own bean on purpose. {@code @Cacheable} is applied by a proxy, so a
 * call from another method of the <em>same</em> class bypasses it entirely. Keeping the cached
 * method here forces {@link TemplateService} to call it through the proxy, which is the only
 * way the Redis cache is actually consulted.
 */
@Component
@RequiredArgsConstructor
public class TemplateLookup {

    private final TemplateRepository templateRepository;

    @Cacheable(cacheNames = "templates", key = "#code + ':' + #channel")
    public NotificationTemplate find(String code, Channel channel) {
        return templateRepository.findByCodeAndChannelAndActiveIsTrue(code, channel)
                .orElseThrow(() -> new TemplateNotFoundException(code, channel));
    }

    /** Call after editing a template so workers stop serving the stale version. */
    @CacheEvict(cacheNames = "templates", key = "#code + ':' + #channel")
    public void evict(String code, Channel channel) {
        // Annotation-driven; nothing to do here.
    }
}