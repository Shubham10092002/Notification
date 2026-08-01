package com.common.Notification.api;

import com.common.Notification.exception.RateLimitExceededException;
import com.common.Notification.exception.TemplateNotFoundException;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Maps failures to RFC 7807 problem responses.
 *
 * <p>Two rules hold throughout. Status codes must let a caller tell "retry me" from "fix your
 * request", otherwise client retry logic is guesswork and error-rate alerting is meaningless.
 * And no response may echo exception internals: recipients are PII and the rest of the codebase
 * masks them, so an unhandled exception leaking a column value would quietly undo that.
 */
@RestControllerAdvice
@Slf4j
public class ApiExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ProblemDetail onValidationFailure(MethodArgumentNotValidException ex) {
        Map<String, String> errors = new LinkedHashMap<>();
        ex.getBindingResult().getFieldErrors()
                .forEach(error -> errors.put(error.getField(), error.getDefaultMessage()));

        ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        problem.setTitle("Invalid notification request");
        // Field names and constraint messages only; never the rejected values.
        problem.setProperty("errors", errors);
        return problem;
    }

    /**
     * Raised by method validation on {@code NotificationService.submit}. This is where REST
     * validation failures now surface, since the constraints live on the command rather than on
     * the request DTO.
     */
    @ExceptionHandler(ConstraintViolationException.class)
    ProblemDetail onConstraintViolation(ConstraintViolationException ex) {
        Map<String, String> errors = new LinkedHashMap<>();
        ex.getConstraintViolations().forEach(violation -> {
            String path = violation.getPropertyPath().toString();
            // Method validation prefixes the path with "submit.command."; callers care about
            // the field, not our method signature.
            String field = path.substring(path.lastIndexOf('.') + 1);
            errors.put(field, violation.getMessage());
        });

        ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        problem.setTitle("Invalid notification request");
        // Field names and constraint messages only; getInvalidValue() is deliberately not used,
        // because one of those values is the recipient.
        problem.setProperty("errors", errors);
        return problem;
    }

    /** Malformed JSON, or an unknown enum value such as a channel we do not support. */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    ProblemDetail onUnreadableBody(HttpMessageNotReadableException ex) {
        log.debug("Unreadable request body", ex);
        ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        problem.setTitle("Malformed request body");
        problem.setDetail("Request body could not be parsed. Check JSON syntax and that "
                + "'channel' is one of EMAIL, SMS.");
        return problem;
    }

    /**
     * 422 rather than 404: the request itself is well-formed, but it names a template that does
     * not exist, which is a caller-side mistake worth distinguishing from a bad URL.
     */
    @ExceptionHandler(TemplateNotFoundException.class)
    ProblemDetail onTemplateNotFound(TemplateNotFoundException ex) {
        ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.UNPROCESSABLE_CONTENT);
        problem.setTitle("Unknown template");
        problem.setDetail(ex.getMessage());
        return problem;
    }

    /**
     * Reachable only if a caller-facing path ever consults the limiter directly — delivery-side
     * throttling is handled by Kafka redelivery, not by an HTTP response. Mapped anyway so it can
     * never fall through to a 500, which would tell callers to stop rather than to retry.
     */
    @ExceptionHandler(RateLimitExceededException.class)
    ProblemDetail onRateLimited(RateLimitExceededException ex) {
        ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.TOO_MANY_REQUESTS);
        problem.setTitle("Rate limit exceeded");
        problem.setDetail("Notification channel is over its send budget. Retry shortly.");
        return problem;
    }

    /**
     * A duplicate-key collision that could not be resolved to a winning row.
     *
     * <p>409, not 500 — the request conflicts with existing state rather than breaking the
     * server. The exception message is logged but never returned: it can contain column values,
     * and one of those columns is the recipient.
     */
    @ExceptionHandler(DataIntegrityViolationException.class)
    ProblemDetail onDataIntegrityViolation(DataIntegrityViolationException ex) {
        log.warn("Data integrity violation handling notification request", ex);
        ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.CONFLICT);
        problem.setTitle("Conflicting notification request");
        problem.setDetail("The request conflicts with an existing notification.");
        return problem;
    }

    /**
     * Catch-all. Returns a correlation id instead of any internal detail, so support can find the
     * stack trace in the logs without the response carrying it.
     */
    @ExceptionHandler(Exception.class)
    ProblemDetail onUnexpected(Exception ex) {
        String errorId = UUID.randomUUID().toString();
        log.error("Unhandled exception errorId={}", errorId, ex);

        ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.INTERNAL_SERVER_ERROR);
        problem.setTitle("Internal error");
        problem.setDetail("Unexpected error. Quote errorId " + errorId + " to support.");
        problem.setProperty("errorId", errorId);
        return problem;
    }
}