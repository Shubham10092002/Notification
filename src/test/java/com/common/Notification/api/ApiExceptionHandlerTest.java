package com.common.Notification.api;

import com.common.Notification.exception.RateLimitExceededException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;

import static org.assertj.core.api.Assertions.assertThat;

class ApiExceptionHandlerTest {

    private final ApiExceptionHandler handler = new ApiExceptionHandler();

    @Test
    @DisplayName("rate limiting maps to 429 so callers know to retry rather than stop")
    void mapsRateLimitTo429() {
        ProblemDetail problem = handler.onRateLimited(
                new RateLimitExceededException("channel over budget"));

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
    }

    @Test
    @DisplayName("a duplicate-key failure maps to 409 and never echoes the exception message")
    void mapsDataIntegrityTo409WithoutLeaking() {
        DataIntegrityViolationException ex = new DataIntegrityViolationException(
                "Duplicate entry 'jane@example.com' for key 'ux_notification_request_id'");

        ProblemDetail problem = handler.onDataIntegrityViolation(ex);

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.CONFLICT.value());
        // The driver's message embeds column values, and one of those columns is the recipient.
        assertThat(problem.getDetail()).doesNotContain("jane@example.com");
    }

    @Test
    @DisplayName("an unexpected error returns a correlation id, not internals")
    void mapsUnexpectedTo500WithCorrelationId() {
        ProblemDetail problem = handler.onUnexpected(
                new IllegalStateException("connection pool exhausted at com.internal.Pool:42"));

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
        assertThat(problem.getDetail()).doesNotContain("connection pool exhausted");
        assertThat(problem.getProperties()).containsKey("errorId");
        assertThat(problem.getDetail()).contains(
                String.valueOf(problem.getProperties().get("errorId")));
    }

    @Test
    @DisplayName("each unexpected error gets its own correlation id")
    void correlationIdsAreUnique() {
        Object first = handler.onUnexpected(new RuntimeException("a")).getProperties().get("errorId");
        Object second = handler.onUnexpected(new RuntimeException("b")).getProperties().get("errorId");

        assertThat(first).isNotEqualTo(second);
    }
}