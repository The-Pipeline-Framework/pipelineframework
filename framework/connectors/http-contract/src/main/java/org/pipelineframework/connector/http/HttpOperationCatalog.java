package org.pipelineframework.connector.http;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.ConnectorOperationKind;

/** Strict multi-resource loader for immutable HTTP operation pins. */
public final class HttpOperationCatalog {
    public static final String RESOURCE_PATH = "META-INF/pipeline/http-operations.json";
    public static final int SCHEMA_VERSION = 1;
    public static final int MAX_OPERATIONS = 512;

    private final List<HttpOperationPin> operations;

    public HttpOperationCatalog(List<HttpOperationPin> operations) {
        Objects.requireNonNull(operations, "HTTP operation pins must not be null");
        if (operations.size() > MAX_OPERATIONS) {
            throw new IllegalArgumentException("HTTP operation pin catalogue exceeds operation limit");
        }
        Set<String> identities = new HashSet<>();
        this.operations = operations.stream().map(operation ->
            Objects.requireNonNull(operation, "HTTP operation pin must not be null"))
            .sorted(Comparator.comparing(HttpOperationPin::identity)).peek(operation -> {
                if (!identities.add(operation.identity())) {
                    throw new IllegalArgumentException("duplicate pinned HTTP operation: " + operation.identity());
                }
            }).toList();
    }

    public List<HttpOperationPin> operations() {
        return operations;
    }

    public static HttpOperationCatalog load(ClassLoader classLoader) {
        Objects.requireNonNull(classLoader, "HTTP metadata classloader must not be null");
        try {
            Enumeration<URL> resources = classLoader.getResources(RESOURCE_PATH);
            List<URL> ordered = new ArrayList<>();
            while (resources.hasMoreElements()) ordered.add(resources.nextElement());
            ordered.sort(Comparator.comparing(URL::toExternalForm));
            Map<String, HttpOperationPin> pins = new LinkedHashMap<>();
            for (URL resource : ordered) {
                try (var stream = resource.openStream()) {
                    byte[] bytes = stream.readNBytes(HttpPinnedJson.MAX_RESOURCE_BYTES + 1);
                    if (bytes.length > HttpPinnedJson.MAX_RESOURCE_BYTES) {
                        throw new IllegalArgumentException("HTTP operation pin resource exceeds size limit");
                    }
                    for (HttpOperationPin pin : read(new String(bytes, StandardCharsets.UTF_8)).operations()) {
                        HttpOperationPin previous = pins.putIfAbsent(pin.identity(), pin);
                        if (previous != null && !previous.equals(pin)) {
                            throw new IllegalArgumentException(
                                "conflicting pinned HTTP operation: " + pin.identity());
                        }
                    }
                }
            }
            return new HttpOperationCatalog(List.copyOf(pins.values()));
        } catch (IOException failure) {
            throw new IllegalStateException("unable to load pinned HTTP operations", failure);
        }
    }

    public String json() {
        var root = JsonNodeFactory.instance.objectNode();
        root.put("schemaVersion", SCHEMA_VERSION);
        root.put("provider", "http.client");
        var array = root.putArray("operations");
        operations.forEach(operation -> array.add(operation.toJson()));
        return HttpPinnedJson.canonicalize(root) + "\n";
    }

    public static HttpOperationCatalog read(String json) {
        JsonNode root = HttpPinnedJson.parse(json);
        fields(root, Set.of("schemaVersion", "provider", "operations"), "HTTP operation catalogue");
        if (!root.path("schemaVersion").isInt() || root.path("schemaVersion").intValue() != SCHEMA_VERSION
            || !"http.client".equals(requiredText(root, "provider"))) {
            throw new IllegalArgumentException("unsupported pinned HTTP operation catalogue");
        }
        JsonNode operations = root.path("operations");
        if (!operations.isArray() || operations.size() > MAX_OPERATIONS) {
            throw new IllegalArgumentException("pinned HTTP operations must be a bounded array");
        }
        List<HttpOperationPin> result = new ArrayList<>();
        operations.forEach(node -> result.add(operation(node)));
        return new HttpOperationCatalog(result);
    }

    private static HttpOperationPin operation(JsonNode node) {
        fields(node, Set.of("operation", "kind", "majorVersion", "input", "output", "method", "path",
            "parameters", "requestBody", "responses", "security", "requestSchema", "requestSchemaFingerprint",
            "requestMapping",
            "providerIdempotencyKey", "sourceFingerprint", "operationFingerprint"), "HTTP operation pin");
        List<HttpParameterPin> parameters = new ArrayList<>();
        array(node, "parameters").forEach(parameter -> parameters.add(parameter(parameter)));
        List<HttpResponsePin> responses = new ArrayList<>();
        array(node, "responses").forEach(response -> responses.add(response(response)));
        List<HttpSecurityRequirement> security = new ArrayList<>();
        array(node, "security").forEach(requirement -> {
            fields(requirement, Set.of("scheme", "scopes", "targets"), "HTTP security requirement");
            List<String> scopes = new ArrayList<>();
            array(requirement, "scopes").forEach(scope -> scopes.add(text(scope, "HTTP security scope")));
            List<HttpAuthorizationTarget> targets = new ArrayList<>();
            array(requirement, "targets").forEach(target -> {
                fields(target, Set.of("location", "name"), "HTTP authorization target");
                targets.add(new HttpAuthorizationTarget(
                    enumValue(target, "location", HttpParameterLocation.class), requiredText(target, "name")));
            });
            security.add(new HttpSecurityRequirement(requiredText(requirement, "scheme"), scopes, targets));
        });
        Optional<HttpRequestBodyPin> body = node.has("requestBody")
            ? Optional.of(body(node.get("requestBody"))) : Optional.empty();
        Optional<HttpProviderIdempotencyKeyTarget> idempotency = node.has("providerIdempotencyKey")
            ? Optional.of(idempotency(node.get("providerIdempotencyKey"))) : Optional.empty();
        return new HttpOperationPin(
            requiredText(node, "operation"), ConnectorOperationKind.of(requiredText(node, "kind")),
            requiredInt(node, "majorVersion"), requiredText(node, "input"), requiredText(node, "output"),
            requiredText(node, "method"), requiredText(node, "path"), parameters, body, responses,
            new HttpSecurityConstraint(security), schema(node, "requestSchema", "requestSchemaFingerprint"),
            requiredText(node, "requestMapping"),
            idempotency, requiredText(node, "sourceFingerprint"),
            requiredText(node, "operationFingerprint"));
    }

    private static HttpParameterPin parameter(JsonNode node) {
        fields(node, Set.of("name", "location", "sourcePath", "style", "explode", "required", "allowReserved",
            "schema", "schemaFingerprint"), "HTTP parameter pin");
        return new HttpParameterPin(requiredText(node, "name"), enumValue(node, "location", HttpParameterLocation.class),
            requiredText(node, "sourcePath"), enumValue(node, "style", HttpParameterStyle.class),
            requiredBoolean(node, "explode"), requiredBoolean(node, "required"),
            requiredBoolean(node, "allowReserved"), schema(node));
    }

    private static HttpRequestBodyPin body(JsonNode node) {
        fields(node, Set.of("mediaType", "sourcePath", "required", "schema", "schemaFingerprint"),
            "HTTP request body pin");
        return new HttpRequestBodyPin(requiredText(node, "mediaType"), optionalText(node, "sourcePath"),
            requiredBoolean(node, "required"), schema(node));
    }

    private static HttpResponsePin response(JsonNode node) {
        fields(node, Set.of("status", "mediaType", "outcome", "mapping", "code", "confirmation", "schema",
            "schemaFingerprint"), "HTTP response pin");
        Optional<HttpWireSchema> schema = node.has("schema") ? Optional.of(schema(node)) : Optional.empty();
        return new HttpResponsePin(requiredText(node, "status"), optionalText(node, "mediaType"),
            enumValue(node, "outcome", HttpResponseOutcome.class), optionalText(node, "mapping"), optionalText(node, "code"),
            optionalEnum(node, "confirmation", CommandMachineConfirmation.class), schema);
    }

    private static HttpProviderIdempotencyKeyTarget idempotency(JsonNode node) {
        fields(node, Set.of("location", "name"), "HTTP provider idempotency-key target");
        return new HttpProviderIdempotencyKeyTarget(enumValue(node, "location", HttpParameterLocation.class),
            requiredText(node, "name"));
    }

    private static HttpWireSchema schema(JsonNode node) {
        return schema(node, "schema", "schemaFingerprint");
    }

    private static HttpWireSchema schema(JsonNode node, String schemaField, String fingerprintField) {
        if (!node.has(schemaField)) throw new IllegalArgumentException("HTTP wire schema is required");
        return new HttpWireSchema(HttpPinnedJson.canonicalize(node.get(schemaField)),
            requiredText(node, fingerprintField));
    }

    private static JsonNode array(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isArray()) throw new IllegalArgumentException("HTTP pin field '" + field + "' must be an array");
        return value;
    }

    private static void fields(JsonNode node, Set<String> allowed, String subject) {
        if (!node.isObject()) throw new IllegalArgumentException(subject + " must be an object");
        node.fieldNames().forEachRemaining(field -> {
            if (!allowed.contains(field)) throw new IllegalArgumentException(subject + " contains unknown field '" + field + "'");
        });
    }

    private static String requiredText(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isTextual() || value.textValue().isBlank()) {
            throw new IllegalArgumentException("HTTP pin field '" + field + "' must be a non-blank string");
        }
        return value.textValue();
    }

    private static Optional<String> optionalText(JsonNode node, String field) {
        return node.has(field) ? Optional.of(requiredText(node, field)) : Optional.empty();
    }

    private static int requiredInt(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isIntegralNumber() || !value.canConvertToInt()) {
            throw new IllegalArgumentException("HTTP pin field '" + field + "' must be an integer");
        }
        return value.intValue();
    }

    private static boolean requiredBoolean(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isBoolean()) throw new IllegalArgumentException("HTTP pin field '" + field + "' must be boolean");
        return value.booleanValue();
    }

    private static String text(JsonNode node, String subject) {
        if (!node.isTextual() || node.textValue().isBlank()) {
            throw new IllegalArgumentException(subject + " must be a non-blank string");
        }
        return node.textValue();
    }

    private static <E extends Enum<E>> E enumValue(JsonNode node, String field, Class<E> type) {
        try {
            return Enum.valueOf(type, requiredText(node, field));
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("unsupported HTTP pin " + field + ": " + node.path(field).asText(), failure);
        }
    }

    private static <E extends Enum<E>> Optional<E> optionalEnum(JsonNode node, String field, Class<E> type) {
        return node.has(field) ? Optional.of(enumValue(node, field, type)) : Optional.empty();
    }
}
