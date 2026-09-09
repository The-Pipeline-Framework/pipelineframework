package org.pipelineframework.connector.http;

import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.connector.CommandMachineConfirmation;

/** Explicit status/media interpretation for one HTTP response variant. */
public record HttpResponsePin(
    String status,
    Optional<String> mediaType,
    HttpResponseOutcome outcome,
    Optional<String> mappingKey,
    Optional<String> code,
    Optional<CommandMachineConfirmation> confirmation,
    Optional<HttpWireSchema> schema
) {
    public HttpResponsePin {
        status = HttpParameterPin.requireText(status, "HTTP response status");
        if (!status.matches("(?:[1-5][0-9][0-9]|[1-5]XX|default)")) {
            throw new IllegalArgumentException("HTTP response status must be exact, range, or default: " + status);
        }
        mediaType = Objects.requireNonNull(mediaType, "HTTP response media type must not be null")
            .map(HttpRequestBodyPin::mediaType);
        outcome = Objects.requireNonNull(outcome, "HTTP response outcome must not be null");
        mappingKey = Objects.requireNonNull(mappingKey, "HTTP response mapping key must not be null")
            .map(value -> org.pipelineframework.connector.ConnectorProviderId.of(value).value());
        code = Objects.requireNonNull(code, "HTTP response code must not be null").map(value -> {
            String normalized = HttpParameterPin.requireText(value, "HTTP response outcome code");
            if (!normalized.matches("[a-z][a-z0-9-]{0,127}")) {
                throw new IllegalArgumentException("HTTP response outcome code must be a stable lowercase identity");
            }
            return normalized;
        });
        confirmation = Objects.requireNonNull(confirmation, "HTTP response confirmation must not be null");
        schema = Objects.requireNonNull(schema, "HTTP response schema must not be null");
        boolean successful = outcome == HttpResponseOutcome.RESULT || outcome == HttpResponseOutcome.SUCCEEDED;
        if (successful && schema.isEmpty()) {
            throw new IllegalArgumentException("successful HTTP response outcomes require a wire schema");
        }
        if (successful && mappingKey.isEmpty()) {
            throw new IllegalArgumentException("successful HTTP response outcomes require a representation mapping key");
        }
        if (!successful && mappingKey.isPresent()) {
            throw new IllegalArgumentException("failure and empty HTTP outcomes cannot declare a representation mapping key");
        }
        boolean commandSucceeded = outcome == HttpResponseOutcome.SUCCEEDED;
        if (commandSucceeded != confirmation.isPresent()) {
            throw new IllegalArgumentException("only succeeded HTTP responses declare Command confirmation");
        }
        boolean successfulOutcome = switch (outcome) {
            case RETRYABLE_FAILURE, TERMINAL_FAILURE, AUTHENTICATION_REQUIRED, EMPTY -> false;
            default -> true;
        };
        if (successfulOutcome == code.isPresent()) {
            throw new IllegalArgumentException("only non-result HTTP response outcomes require an outcome code");
        }
    }

    public boolean matches(int value) {
        if (status.equals("default")) return true;
        if (status.endsWith("XX")) return value / 100 == status.charAt(0) - '0';
        return value == Integer.parseInt(status);
    }
}
