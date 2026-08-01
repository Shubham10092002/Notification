package com.common.Notification.domain;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The command carries the constraints for <em>both</em> entry points, enforced by method
 * validation on {@code NotificationService}. These tests cover the rules once, which is the whole
 * point of having one command type: the REST and Kafka paths cannot drift into enforcing
 * different contracts.
 */
class SendNotificationCommandValidationTest {

    private static ValidatorFactory factory;
    private static Validator validator;

    @BeforeAll
    static void startValidator() {
        factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @AfterAll
    static void stopValidator() {
        factory.close();
    }

    private SendNotificationCommand command(String requestId, Channel channel,
                                            String recipient, String templateCode) {
        return new SendNotificationCommand(requestId, "order-service", channel,
                recipient, templateCode, Map.of("name", "Jane"));
    }

    private Set<String> violatedFields(SendNotificationCommand command) {
        return validator.validate(command).stream()
                .map(violation -> violation.getPropertyPath().toString())
                .collect(Collectors.toSet());
    }

    @Test
    @DisplayName("a well-formed command passes")
    void acceptsValidCommand() {
        assertThat(validator.validate(
                command("req-1", Channel.EMAIL, "jane@example.com", "WELCOME"))).isEmpty();
    }

    @Test
    @DisplayName("a null requestId is rejected — it would silently defeat idempotency")
    void rejectsNullRequestId() {
        // findByRequestId(null) matches nothing, so without this every redelivery of an event
        // would insert a new row and send the notification again.
        assertThat(violatedFields(command(null, Channel.EMAIL, "jane@example.com", "WELCOME")))
                .contains("requestId");
    }

    @Test
    @DisplayName("a blank requestId is rejected as well as a null one")
    void rejectsBlankRequestId() {
        assertThat(violatedFields(command("   ", Channel.EMAIL, "jane@example.com", "WELCOME")))
                .contains("requestId");
    }

    @Test
    void rejectsNullChannel() {
        assertThat(violatedFields(command("req-1", null, "jane@example.com", "WELCOME")))
                .contains("channel");
    }

    @Test
    void rejectsMissingRecipient() {
        assertThat(violatedFields(command("req-1", Channel.EMAIL, null, "WELCOME")))
                .contains("recipient");
    }

    @Test
    void rejectsMissingTemplateCode() {
        assertThat(violatedFields(command("req-1", Channel.EMAIL, "jane@example.com", null)))
                .contains("templateCode");
    }

    @Test
    @DisplayName("an oversized recipient is rejected before it hits the column limit")
    void rejectsOversizedRecipient() {
        String tooLong = "a".repeat(321) + "@example.com";

        assertThat(violatedFields(command("req-1", Channel.EMAIL, tooLong, "WELCOME")))
                .contains("recipient");
    }

    @Test
    @DisplayName("constraint messages never contain the rejected value")
    void messagesDoNotLeakValues() {
        Set<ConstraintViolation<SendNotificationCommand>> violations =
                validator.validate(command(null, Channel.EMAIL, "jane@example.com", "WELCOME"));

        // Recipients are PII; messages surface in API responses and DLT headers.
        assertThat(violations).allSatisfy(violation ->
                assertThat(violation.getMessage()).doesNotContain("jane@example.com"));
    }
}