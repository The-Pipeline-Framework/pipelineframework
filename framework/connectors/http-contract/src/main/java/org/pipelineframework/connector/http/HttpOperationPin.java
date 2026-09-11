package org.pipelineframework.connector.http;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;

/** Immutable runtime pin for one release-selected HTTP capability. */
public record HttpOperationPin(
    String operation,
    ConnectorOperationKind kind,
    int majorVersion,
    String inputType,
    String outputType,
    String method,
    String relativePathTemplate,
    List<HttpParameterPin> parameters,
    Optional<HttpRequestBodyPin> requestBody,
    List<HttpResponsePin> responses,
    HttpSecurityConstraint security,
    HttpWireSchema requestSchema,
    String requestMappingKey,
    Optional<HttpProviderIdempotencyKeyTarget> providerIdempotencyKey,
    List<HttpCallbackPin> callbacks,
    String sourceFingerprint,
    String operationFingerprint
) {
    private static final Pattern PATH_SLOT = Pattern.compile("\\{([^{}]+)}");
    private static final Set<String> METHODS = Set.of("GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS");

    public HttpOperationPin(
        String operation,
        ConnectorOperationKind kind,
        int majorVersion,
        String inputType,
        String outputType,
        String method,
        String relativePathTemplate,
        List<HttpParameterPin> parameters,
        Optional<HttpRequestBodyPin> requestBody,
        List<HttpResponsePin> responses,
        HttpSecurityConstraint security,
        HttpWireSchema requestSchema,
        String requestMappingKey,
        Optional<HttpProviderIdempotencyKeyTarget> providerIdempotencyKey,
        String sourceFingerprint
    ) {
        this(operation, kind, majorVersion, inputType, outputType, method, relativePathTemplate, parameters,
            requestBody, responses, security, requestSchema, requestMappingKey, providerIdempotencyKey,
            List.of(), sourceFingerprint);
    }

    public HttpOperationPin(String operation, ConnectorOperationKind kind, int majorVersion,
        String inputType, String outputType, String method, String relativePathTemplate,
        List<HttpParameterPin> parameters, Optional<HttpRequestBodyPin> requestBody,
        List<HttpResponsePin> responses, HttpSecurityConstraint security, HttpWireSchema requestSchema,
        String requestMappingKey, Optional<HttpProviderIdempotencyKeyTarget> providerIdempotencyKey,
        String sourceFingerprint, String operationFingerprint) {
        this(operation, kind, majorVersion, inputType, outputType, method, relativePathTemplate, parameters,
            requestBody, responses, security, requestSchema, requestMappingKey, providerIdempotencyKey,
            List.of(), sourceFingerprint, operationFingerprint);
    }

    public HttpOperationPin(String operation, ConnectorOperationKind kind, int majorVersion,
        String inputType, String outputType, String method, String relativePathTemplate,
        List<HttpParameterPin> parameters, Optional<HttpRequestBodyPin> requestBody,
        List<HttpResponsePin> responses, HttpSecurityConstraint security, HttpWireSchema requestSchema,
        String requestMappingKey, Optional<HttpProviderIdempotencyKeyTarget> providerIdempotencyKey,
        List<HttpCallbackPin> callbacks, String sourceFingerprint) {
        this(operation, kind, majorVersion, inputType, outputType, method, relativePathTemplate, parameters,
            requestBody, responses, security, requestSchema, requestMappingKey, providerIdempotencyKey,
            callbacks, sourceFingerprint, fingerprint(
                ConnectorProviderId.of(operation).value(), kind, majorVersion,
                HttpParameterPin.requireText(inputType, "canonical HTTP input type"),
                HttpParameterPin.requireText(outputType, "canonical HTTP output type"),
                HttpParameterPin.requireText(method, "HTTP method").toUpperCase(Locale.ROOT),
                relativePath(relativePathTemplate),
                Objects.requireNonNull(parameters, "HTTP parameters must not be null"),
                Objects.requireNonNull(requestBody, "HTTP request body must not be null"),
                Objects.requireNonNull(responses, "HTTP responses must not be null"),
                Objects.requireNonNull(security, "HTTP security constraint must not be null"),
                Objects.requireNonNull(requestSchema, "HTTP request wire schema must not be null"),
                ConnectorProviderId.of(requestMappingKey).value(),
                Objects.requireNonNull(providerIdempotencyKey, "provider idempotency-key target must not be null"),
                Objects.requireNonNull(callbacks, "HTTP callbacks must not be null"),
                digest(sourceFingerprint, "HTTP source fingerprint")));
    }

    public HttpOperationPin {
        operation = ConnectorProviderId.of(operation).value();
        kind = Objects.requireNonNull(kind, "HTTP operation kind must not be null");
        if (!kind.equals(ConnectorOperationKind.QUERY) && !kind.equals(ConnectorOperationKind.COMMAND)) {
            throw new IllegalArgumentException("pinned HTTP operation must be Query or Command");
        }
        if (majorVersion < 1) throw new IllegalArgumentException("HTTP operation major version must be positive");
        inputType = HttpParameterPin.requireText(inputType, "canonical HTTP input type");
        outputType = HttpParameterPin.requireText(outputType, "canonical HTTP output type");
        method = HttpParameterPin.requireText(method, "HTTP method").toUpperCase(Locale.ROOT);
        if (!METHODS.contains(method)) throw new IllegalArgumentException("unsupported pinned HTTP method: " + method);
        relativePathTemplate = relativePath(relativePathTemplate);
        parameters = List.copyOf(Objects.requireNonNull(parameters, "HTTP parameters must not be null"));
        requestBody = Objects.requireNonNull(requestBody, "HTTP request body must not be null");
        responses = List.copyOf(Objects.requireNonNull(responses, "HTTP responses must not be null"));
        security = Objects.requireNonNull(security, "HTTP security constraint must not be null");
        requestSchema = Objects.requireNonNull(requestSchema, "HTTP request wire schema must not be null");
        requestMappingKey = ConnectorProviderId.of(requestMappingKey).value();
        providerIdempotencyKey = Objects.requireNonNull(providerIdempotencyKey,
            "provider idempotency-key target must not be null");
        callbacks = Objects.requireNonNull(callbacks, "HTTP callbacks must not be null").stream()
            .sorted(java.util.Comparator.comparing(HttpCallbackPin::id)).toList();
        if (!callbacks.isEmpty() && !kind.equals(ConnectorOperationKind.COMMAND)) {
            throw new IllegalArgumentException("HTTP callbacks require a Command operation");
        }
        if (callbacks.size() > 16 || callbacks.stream().map(HttpCallbackPin::id).distinct().count() != callbacks.size()
            || callbacks.stream().map(callback -> callback.target().pointer()).distinct().count() != callbacks.size()) {
            throw new IllegalArgumentException("HTTP callbacks must have bounded, unique IDs and targets");
        }
        for (HttpCallbackPin callback : callbacks) {
            if (!callback.operation().equals(operation) || callback.majorVersion() != majorVersion) {
                throw new IllegalArgumentException("HTTP callback belongs to a different Command");
            }
            callback.target().validate(parameters, requestBody, requestSchema, security, providerIdempotencyKey);
        }
        sourceFingerprint = digest(sourceFingerprint, "HTTP source fingerprint");
        operationFingerprint = digest(operationFingerprint, "HTTP operation fingerprint");
        validateParameters(relativePathTemplate, parameters, security, providerIdempotencyKey);
        validateResponses(kind, responses);
        if (!kind.equals(ConnectorOperationKind.COMMAND) && providerIdempotencyKey.isPresent()) {
            throw new IllegalArgumentException("provider idempotency-key projection requires a Command operation");
        }
        String expected = fingerprint(operation, kind, majorVersion, inputType, outputType, method,
            relativePathTemplate, parameters, requestBody, responses, security, requestSchema, requestMappingKey,
            providerIdempotencyKey, callbacks, sourceFingerprint);
        if (!expected.equals(operationFingerprint)) {
            throw new IllegalArgumentException("HTTP operation fingerprint mismatch");
        }
    }

    public String identity() {
        return kind.value() + ":" + operation + ":" + majorVersion;
    }

    public List<String> runtimeSuppliedPaths() {
        return callbacks.stream().map(callback -> callback.target().pointer()).sorted().toList();
    }

    public Optional<HttpCallbackPin> selectCallback(
        Optional<org.pipelineframework.connector.ConnectorCallbackContext> context) {
        Objects.requireNonNull(context, "HTTP callback context");
        if (context.isEmpty()) {
            if (callbacks.stream().anyMatch(HttpCallbackPin::required)) {
                throw new IllegalArgumentException("HTTP Command requires its selected callback context");
            }
            return Optional.empty();
        }
        HttpCallbackPin selected = callbacks.stream().filter(callback -> callback.id().equals(context.orElseThrow().callbackId()))
            .findFirst().orElseThrow(() -> new IllegalArgumentException("HTTP Command callback context is not pinned"));
        if (callbacks.stream().anyMatch(callback -> callback.required() && !callback.id().equals(selected.id()))) {
            throw new IllegalArgumentException("HTTP Command has another required callback context");
        }
        return Optional.of(selected);
    }

    public ObjectNode toJson() {
        ObjectNode node = fields(operation, kind, majorVersion, inputType, outputType, method, relativePathTemplate,
            parameters, requestBody, responses, security, requestSchema, requestMappingKey,
            providerIdempotencyKey, callbacks, sourceFingerprint);
        node.put("operationFingerprint", operationFingerprint);
        return node;
    }

    private static void validateParameters(
        String path,
        List<HttpParameterPin> parameters,
        HttpSecurityConstraint security,
        Optional<HttpProviderIdempotencyKeyTarget> idempotencyKey
    ) {
        Set<String> identities = new HashSet<>();
        Set<String> pathParameters = new HashSet<>();
        for (HttpParameterPin parameter : parameters) {
            Objects.requireNonNull(parameter, "HTTP parameter must not be null");
            String key = parameter.location() + ":" + parameter.name().toLowerCase(Locale.ROOT);
            if (!identities.add(key)) throw new IllegalArgumentException("duplicate HTTP parameter: " + key);
            if (parameter.location() == HttpParameterLocation.PATH) pathParameters.add(parameter.name());
        }
        Set<String> slots = new HashSet<>();
        Matcher matcher = PATH_SLOT.matcher(path);
        while (matcher.find()) slots.add(matcher.group(1));
        if (!slots.equals(pathParameters)) {
            throw new IllegalArgumentException("HTTP path template slots must exactly match pinned path parameters");
        }
        security.requirements().stream().flatMap(requirement -> requirement.targets().stream()).forEach(target -> {
            String key = target.location() + ":" + target.name().toLowerCase(Locale.ROOT);
            if (identities.contains(key)) {
                throw new IllegalArgumentException("HTTP authorization target collides with an input parameter: " + key);
            }
        });
        idempotencyKey.ifPresent(target -> {
            String key = target.location() + ":" + target.name().toLowerCase(Locale.ROOT);
            if (identities.contains(key)) {
                throw new IllegalArgumentException("provider idempotency-key target collides with an input parameter");
            }
            if (security.requirements().stream().flatMap(requirement -> requirement.targets().stream())
                .anyMatch(auth -> auth.location() == target.location() && auth.name().equalsIgnoreCase(target.name()))) {
                throw new IllegalArgumentException("provider idempotency-key target collides with authorization material");
            }
        });
    }

    private static void validateResponses(ConnectorOperationKind kind, List<HttpResponsePin> responses) {
        if (responses.isEmpty()) throw new IllegalArgumentException("pinned HTTP operation requires response mappings");
        Set<String> variants = new HashSet<>();
        long defaults = 0;
        for (HttpResponsePin response : responses) {
            Objects.requireNonNull(response, "HTTP response pin must not be null");
            String variant = response.status() + ":" + response.mediaType().orElse("");
            if (!variants.add(variant)) throw new IllegalArgumentException("duplicate HTTP response mapping: " + variant);
            if ("default".equals(response.status())) defaults++;
            boolean allowed = kind.equals(ConnectorOperationKind.QUERY)
                ? switch (response.outcome()) {
                    case RESULT, EMPTY, RETRYABLE_FAILURE, TERMINAL_FAILURE, AUTHENTICATION_REQUIRED -> true;
                    case SUCCEEDED -> false;
                }
                : switch (response.outcome()) {
                    case SUCCEEDED, RETRYABLE_FAILURE, TERMINAL_FAILURE, AUTHENTICATION_REQUIRED -> true;
                    case RESULT, EMPTY -> false;
                };
            if (!allowed) throw new IllegalArgumentException(
                "HTTP response outcome " + response.outcome() + " is invalid for " + kind.value());
            if (kind.equals(ConnectorOperationKind.COMMAND)
                && response.outcome() == HttpResponseOutcome.SUCCEEDED
                && response.confirmation().filter(value -> value != CommandMachineConfirmation.NONE).isEmpty()) {
                throw new IllegalArgumentException("successful HTTP Commands must declare a non-NONE confirmation");
            }
        }
        if (defaults > 1) throw new IllegalArgumentException("pinned HTTP operation may declare at most one default response");
    }

    private static String relativePath(String value) {
        String result = HttpParameterPin.requireText(value, "relative HTTP path template");
        if (!result.startsWith("/") || result.startsWith("//") || result.contains("?") || result.contains("#")
            || result.contains("://") || result.matches(".*(?:^|/)\\.{1,2}(?:/|$).*") || result.contains("\\")) {
            throw new IllegalArgumentException("HTTP operation path must be a safe origin-relative path template");
        }
        return result;
    }

    private static String digest(String value, String subject) {
        String result = HttpParameterPin.requireText(value, subject).toLowerCase(Locale.ROOT);
        if (!result.matches("[0-9a-f]{64}")) throw new IllegalArgumentException(subject + " must be SHA-256 hex");
        return result;
    }

    private static String fingerprint(
        String operation,
        ConnectorOperationKind kind,
        int majorVersion,
        String inputType,
        String outputType,
        String method,
        String path,
        List<HttpParameterPin> parameters,
        Optional<HttpRequestBodyPin> body,
        List<HttpResponsePin> responses,
        HttpSecurityConstraint security,
        HttpWireSchema requestSchema,
        String requestMappingKey,
        Optional<HttpProviderIdempotencyKeyTarget> providerIdempotencyKey,
        List<HttpCallbackPin> callbacks,
        String sourceFingerprint
    ) {
        return HttpPinnedJson.sha256(HttpPinnedJson.canonicalize(fields(operation, kind, majorVersion, inputType,
            outputType, method, path, parameters, body, responses, security, requestSchema, requestMappingKey,
            providerIdempotencyKey, callbacks, sourceFingerprint)));
    }

    private static ObjectNode fields(
        String operation,
        ConnectorOperationKind kind,
        int majorVersion,
        String inputType,
        String outputType,
        String method,
        String path,
        List<HttpParameterPin> parameters,
        Optional<HttpRequestBodyPin> body,
        List<HttpResponsePin> responses,
        HttpSecurityConstraint security,
        HttpWireSchema requestSchema,
        String requestMappingKey,
        Optional<HttpProviderIdempotencyKeyTarget> providerIdempotencyKey,
        List<HttpCallbackPin> callbacks,
        String sourceFingerprint
    ) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("operation", operation);
        node.put("kind", kind.value());
        node.put("majorVersion", majorVersion);
        node.put("input", inputType);
        node.put("output", outputType);
        node.put("method", method);
        node.put("path", path);
        ArrayNode parameterNodes = node.putArray("parameters");
        parameters.forEach(parameter -> parameterNodes.add(parameterJson(parameter)));
        body.ifPresent(value -> node.set("requestBody", bodyJson(value)));
        ArrayNode responseNodes = node.putArray("responses");
        responses.forEach(response -> responseNodes.add(responseJson(response)));
        node.set("security", security.toJson());
        if (!callbacks.isEmpty()) {
            ArrayNode callbackNodes = node.putArray("callbacks");
            callbacks.stream().sorted(java.util.Comparator.comparing(HttpCallbackPin::id))
                .forEach(callback -> callbackNodes.add(callback.toJson()));
        }
        node.set("requestSchema", requestSchema.node());
        node.put("requestSchemaFingerprint", requestSchema.sha256());
        node.put("requestMapping", requestMappingKey);
        providerIdempotencyKey.ifPresent(value -> {
            ObjectNode target = node.putObject("providerIdempotencyKey");
            target.put("location", value.location().name());
            target.put("name", value.name());
        });
        node.put("sourceFingerprint", sourceFingerprint);
        return node;
    }

    private static ObjectNode parameterJson(HttpParameterPin value) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("name", value.name());
        node.put("location", value.location().name());
        node.put("sourcePath", value.sourcePath());
        node.put("style", value.style().name());
        node.put("explode", value.explode());
        node.put("required", value.required());
        node.put("allowReserved", value.allowReserved());
        node.set("schema", value.schema().node());
        node.put("schemaFingerprint", value.schema().sha256());
        return node;
    }

    private static ObjectNode bodyJson(HttpRequestBodyPin value) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("mediaType", value.mediaType());
        value.sourcePath().ifPresent(path -> node.put("sourcePath", path));
        node.put("required", value.required());
        node.set("schema", value.schema().node());
        node.put("schemaFingerprint", value.schema().sha256());
        return node;
    }

    private static ObjectNode responseJson(HttpResponsePin value) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("status", value.status());
        value.mediaType().ifPresent(media -> node.put("mediaType", media));
        node.put("outcome", value.outcome().name());
        value.mappingKey().ifPresent(mapping -> node.put("mapping", mapping));
        value.code().ifPresent(code -> node.put("code", code));
        value.confirmation().ifPresent(confirmation -> node.put("confirmation", confirmation.name()));
        value.schema().ifPresent(schema -> {
            node.set("schema", schema.node());
            node.put("schemaFingerprint", schema.sha256());
        });
        return node;
    }
}
