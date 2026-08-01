package com.common.Notification.support;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class RedactionTest {

    @ParameterizedTest
    @CsvSource({
            "jane.doe@example.com, j***e@example.com",
            "ab@example.com,       **@example.com",
            "a@example.com,        *@example.com"
    })
    @DisplayName("email keeps the domain but hides the local part")
    void masksEmail(String input, String expected) {
        assertThat(Redaction.mask(input)).isEqualTo(expected);
    }

    @Test
    @DisplayName("phone keeps only a prefix and the last four digits")
    void masksPhone() {
        assertThat(Redaction.mask("+919876543210")).isEqualTo("+91*****3210");
    }

    @Test
    @DisplayName("short values are fully masked rather than partially exposed")
    void masksShortValues() {
        assertThat(Redaction.mask("1234")).isEqualTo("****");
    }

    @Test
    void handlesNullAndBlank() {
        assertThat(Redaction.mask(null)).isEqualTo("<none>");
        assertThat(Redaction.mask("  ")).isEqualTo("<none>");
    }

    @Test
    @DisplayName("no masked output ever contains the full original value")
    void neverLeaksTheOriginal() {
        String[] inputs = {"jane.doe@example.com", "+919876543210", "user@corp.co"};
        for (String input : inputs) {
            assertThat(Redaction.mask(input)).isNotEqualTo(input);
        }
    }
}