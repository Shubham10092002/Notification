package com.common.Notification.template;

import com.common.Notification.domain.Channel;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The Template Service from the design doc: resolves a template by code + channel and renders
 * it against the caller's variables.
 *
 * <p>Lookups are cached in Redis via {@link TemplateLookup} — a separate bean so the cache
 * proxy is actually applied. Rendering itself is deliberately not cached: the output is
 * per-recipient, so caching it would be both useless and a PII leak.
 */
@Service
@RequiredArgsConstructor
public class TemplateService {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{\\{\\s*([a-zA-Z0-9_.]+)\\s*}}");

    private final TemplateLookup templateLookup;

    public TemplateView find(String code, Channel channel) {
        return templateLookup.find(code, channel);
    }

    public void evict(String code, Channel channel) {
        templateLookup.evict(code, channel);
    }

    public Rendered render(String code, Channel channel, Map<String, Object> variables) {
        TemplateView template = templateLookup.find(code, channel);
        return new Rendered(
                substitute(template.subject(), variables),
                substitute(template.body(), variables)
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