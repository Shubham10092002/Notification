package com.common.Notification.api;

import com.common.Notification.exception.TemplateNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps failures to RFC 7807 problem responses.
 *
 * <p>Messages stay deliberately generic: a caller's bad input should never echo back recipient
 * values or internal state.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ProblemDetail onValidationFailure(MethodArgumentNotValidException ex) {
        Map<String, String> errors = new LinkedHashMap<>();
        ex.getBindingResult().getFieldErrors()
                .forEach(error -> errors.put(error.getField(), error.getDefaultMessage()));

        ProblemDetail problem = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        problem.setTitle("Invalid notification request");
        problem.setProperty("errors", errors);
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
}