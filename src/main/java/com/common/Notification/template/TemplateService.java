package com.common.Notification.template;

import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationTemplate;
import com.common.Notification.domain.TemplateRepository;
import com.common.Notification.exception.TemplateNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The Template Service from the design doc: resolves a template by code + channel and renders
 * it against the caller's variables.
 *
 * <p>Templates change rarely and are read on every single notification, so lookups are cached
 * in Redis (the doc's "Redis Cache" concept). Rendering itself is deliberately not cached —
 * the output is per-recipient and caching it would be both useless and a PII leak.
 */
@Service
@RequiredArgsConstructor
public class TemplateService {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{\\{\\s*([a-zA-Z0-9_.]+)\\s*}}");

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

    public Rendered render(String code, Channel channel, Map<String, Object> variables) {
        NotificationTemplate template = find(code, channel);
        return new Rendered(
                substitute(template.getSubject(), variables),
                substitute(template.getBody(), variables)
        );
    }

    /**
     * Replaces {@code {{key}}} with the matching variable.
     *
     * <p>An unresolved placeholder is left verbatim rather than blanked, so a missing variable
     * shows up loudly in the delivered message instead of silently producing "Hello ,".
     */
    private String substitute(String text, Map<String, Object> variables) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        Map<String, Object> vars = variables == null ? Map.of() : variables;
        Matcher matcher = PLACEHOLDER.matcher(text);
        StringBuilder out = new StringBuilder();
        while (matcher.find()) {
            Object value = vars.get(matcher.group(1));
            String replacement = value == null ? matcher.group(0) : String.valueOf(value);
            matcher.appendReplacement(out, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(out);
        return out.toString();
    }

    /** Rendered output. {@code subject} is null for SMS. */
    public record Rendered(String subject, String body) {
    }
}