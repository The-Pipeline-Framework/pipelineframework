package org.pipelineframework.connector.http;

import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Locale;
import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.CommandConfirmation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.QueryOutcome;

/** Maps only explicitly pinned status/media variants into TPF outcomes. */
final class HttpResponseInterpreter {
    private HttpResponseInterpreter() {
    }

    static QueryOutcome<Object> query(
        HttpOperationPin pin,
        HttpResponse<byte[]> response,
        Class<?> outputType,
        HttpRepresentationBindings representations,
        int maxBytes
    ) {
        HttpResponsePin selected = select(pin, response);
        return switch (selected.outcome()) {
            case RESULT -> new QueryOutcome.Found<>(output(selected, response, outputType, representations, maxBytes));
            case EMPTY -> {
                validateFailureBody(selected, response, maxBytes);
                yield new QueryOutcome.NotFound<>(selected.code().orElseThrow());
            }
            case RETRYABLE_FAILURE -> {
                validateFailureBody(selected, response, maxBytes);
                yield new QueryOutcome.TemporarilyUnavailable<>(selected.code().orElseThrow());
            }
            case AUTHENTICATION_REQUIRED -> {
                validateFailureBody(selected, response, maxBytes);
                yield new QueryOutcome.AuthenticationRequired<>(selected.code().orElseThrow());
            }
            case TERMINAL_FAILURE -> {
                validateFailureBody(selected, response, maxBytes);
                yield new QueryOutcome.TerminalFailure<>(selected.code().orElseThrow());
            }
            case SUCCEEDED -> throw new IllegalStateException("Command response mapping reached Query execution");
        };
    }

    static CommandOutcome<Object> command(
        HttpOperationPin pin,
        HttpResponse<byte[]> response,
        Class<?> outputType,
        HttpRepresentationBindings representations,
        int maxBytes
    ) {
        HttpResponsePin selected = select(pin, response);
        return switch (selected.outcome()) {
            case SUCCEEDED -> new CommandOutcome.Succeeded<>(
                output(selected, response, outputType, representations, maxBytes),
                new CommandConfirmation(selected.confirmation().orElseThrow(), false), java.util.List.of());
            case RETRYABLE_FAILURE -> {
                validateFailureBody(selected, response, maxBytes);
                yield new CommandOutcome.RetryableFailure<>(selected.code().orElseThrow(), java.util.List.of());
            }
            case TERMINAL_FAILURE, AUTHENTICATION_REQUIRED -> {
                validateFailureBody(selected, response, maxBytes);
                yield new CommandOutcome.TerminalFailure<>(selected.code().orElseThrow(), java.util.List.of());
            }
            case RESULT, EMPTY -> throw new IllegalStateException("Query response mapping reached Command execution");
        };
    }

    private static HttpResponsePin select(HttpOperationPin pin, HttpResponse<byte[]> response) {
        String mediaType = response.headers().firstValue("Content-Type")
            .map(value -> value.split(";", 2)[0].trim().toLowerCase(Locale.ROOT)).orElse("");
        return pin.responses().stream()
            .filter(candidate -> status(candidate.status(), response.statusCode()))
            .filter(candidate -> candidate.mediaType().isEmpty() || candidate.mediaType().orElseThrow().equals(mediaType))
            .sorted(Comparator.comparingInt(candidate -> specificity(candidate.status())))
            .findFirst().orElseThrow(() -> new IllegalStateException(
                "unclassified HTTP response status/media after dispatch: " + response.statusCode() + " " + mediaType));
    }

    private static int specificity(String status) {
        if (status.matches("[1-5][0-9][0-9]")) return 0;
        if (status.endsWith("XX")) return 1;
        return 2;
    }

    private static boolean status(String pin, int actual) {
        if ("default".equals(pin)) return true;
        if (pin.endsWith("XX")) return actual / 100 == Character.digit(pin.charAt(0), 10);
        return Integer.parseInt(pin) == actual;
    }

    private static Object output(
        HttpResponsePin responsePin,
        HttpResponse<byte[]> response,
        Class<?> outputType,
        HttpRepresentationBindings representations,
        int maxBytes
    ) {
        JsonNode wire = wire(response, maxBytes);
        HttpWireValueValidator.validate(wire, responsePin.schema().orElseThrow());
        return representations.fromWire(responsePin.mappingKey().orElseThrow(), wire, outputType);
    }

    private static void validateFailureBody(
        HttpResponsePin responsePin,
        HttpResponse<byte[]> response,
        int maxBytes
    ) {
        responsePin.schema().ifPresent(schema -> HttpWireValueValidator.validate(wire(response, maxBytes), schema));
    }

    private static JsonNode wire(HttpResponse<byte[]> response, int maxBytes) {
        byte[] body = Objects.requireNonNull(response.body(), "HTTP response body must not be null");
        if (body.length > maxBytes) throw new IllegalStateException("HTTP response exceeds configured size limit");
        try {
            JsonNode wire = PipelineJson.mapper().readTree(new String(body, StandardCharsets.UTF_8));
            if (wire == null) throw new IllegalStateException("HTTP response body is empty");
            return wire;
        } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
            throw new IllegalStateException("HTTP response is not valid JSON", failure);
        }
    }

}
