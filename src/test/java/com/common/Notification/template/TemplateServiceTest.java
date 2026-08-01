package com.common.Notification.template;

import com.common.Notification.domain.Channel;
import com.common.Notification.domain.NotificationTemplate;
import com.common.Notification.domain.TemplateRepository;
import com.common.Notification.exception.TemplateNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TemplateServiceTest {

    @Mock
    private TemplateRepository templateRepository;

    private TemplateService templateService;

    @BeforeEach
    void setUp() {
        templateService = new TemplateService(templateRepository);
    }

    private void stubTemplate(String subject, String body) {
        NotificationTemplate template = new NotificationTemplate();
        template.setCode("WELCOME");
        template.setChannel(Channel.EMAIL);
        template.setSubject(subject);
        template.setBody(body);
        when(templateRepository.findByCodeAndChannelAndActiveIsTrue("WELCOME", Channel.EMAIL))
                .thenReturn(Optional.of(template));
    }

    @Test
    @DisplayName("substitutes placeholders in both subject and body")
    void rendersPlaceholders() {
        stubTemplate("Welcome {{name}}", "Hi {{name}}, your code is {{code}}.");

        TemplateService.Rendered rendered = templateService.render(
                "WELCOME", Channel.EMAIL, Map.of("name", "Shubham", "code", "4821"));

        assertThat(rendered.subject()).isEqualTo("Welcome Shubham");
        assertThat(rendered.body()).isEqualTo("Hi Shubham, your code is 4821.");
    }

    @Test
    @DisplayName("tolerates whitespace inside the braces")
    void toleratesWhitespaceInPlaceholders() {
        stubTemplate("s", "Hi {{ name }}");

        assertThat(templateService.render("WELCOME", Channel.EMAIL, Map.of("name", "Shubham")).body())
                .isEqualTo("Hi Shubham");
    }

    @Test
    @DisplayName("an unresolved placeholder is left verbatim so the gap is visible")
    void leavesUnknownPlaceholderIntact() {
        stubTemplate("s", "Hi {{name}}, ref {{missing}}");

        assertThat(templateService.render("WELCOME", Channel.EMAIL, Map.of("name", "Shubham")).body())
                .isEqualTo("Hi Shubham, ref {{missing}}");
    }

    @Test
    @DisplayName("null variables map does not blow up")
    void handlesNullVariables() {
        stubTemplate("s", "Hi {{name}}");

        assertThat(templateService.render("WELCOME", Channel.EMAIL, null).body())
                .isEqualTo("Hi {{name}}");
    }

    @Test
    @DisplayName("a variable containing $ or \\ is inserted literally, not as a regex group")
    void treatsSubstitutionAsLiteral() {
        stubTemplate("s", "Amount {{amt}}");
        Map<String, Object> vars = new HashMap<>();
        vars.put("amt", "$1,200\\a");

        assertThat(templateService.render("WELCOME", Channel.EMAIL, vars).body())
                .isEqualTo("Amount $1,200\\a");
    }

    @Test
    void throwsWhenTemplateMissing() {
        when(templateRepository.findByCodeAndChannelAndActiveIsTrue("NOPE", Channel.SMS))
                .thenReturn(Optional.empty());

        assertThatThrownBy(() -> templateService.render("NOPE", Channel.SMS, Map.of()))
                .isInstanceOf(TemplateNotFoundException.class)
                .hasMessageContaining("NOPE");
    }
}