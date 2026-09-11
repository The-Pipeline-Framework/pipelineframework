package org.pipelineframework.connector.http;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pipelineframework.connector.ConnectorCallbackContext;

/** One release-selected field in the normalized initiating request, never a public origin. */
public record HttpCallbackInjectionTarget(Location location, List<String> path, Optional<String> parameterName) {
    public enum Location { BODY, QUERY, HEADER }

    public HttpCallbackInjectionTarget {
        location = Objects.requireNonNull(location, "callback injection location");
        path = List.copyOf(Objects.requireNonNull(path, "callback injection path"));
        if (path.isEmpty() || path.size() > 32 || path.stream().anyMatch(field ->
            field.isEmpty() || field.length() > 256 || field.codePoints().anyMatch(Character::isISOControl))) {
            throw new IllegalArgumentException("callback injection requires a bounded object-field path");
        }
        parameterName = Objects.requireNonNull(parameterName, "callback parameter name")
            .map(value -> HttpParameterPin.requireText(value, "callback parameter name"));
        if ((location == Location.BODY) == parameterName.isPresent()) {
            throw new IllegalArgumentException("only query/header callback targets declare a parameter name");
        }
    }

    public String pointer() {
        return path.stream().map(field -> "/" + field.replace("~", "~0").replace("/", "~1"))
            .reduce("", String::concat);
    }

    public ObjectNode toJson() {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("location", location.name());
        var fields = node.putArray("path");
        path.forEach(fields::add);
        parameterName.ifPresent(value -> node.put("parameter", value));
        return node;
    }

    void validate(List<HttpParameterPin> parameters, Optional<HttpRequestBodyPin> body,
        HttpWireSchema requestSchema, HttpSecurityConstraint security,
        Optional<HttpProviderIdempotencyKeyTarget> idempotency) {
        if (location == Location.BODY) {
            HttpRequestBodyPin requestBody = body.orElseThrow(() ->
                new IllegalArgumentException("body callback target requires a request body"));
            requestBody.sourcePath().ifPresent(source -> {
                List<String> prefix = List.of(source.split("\\."));
                if (path.size() <= prefix.size() || !path.subList(0, prefix.size()).equals(prefix)) {
                    throw new IllegalArgumentException("callback target is outside the pinned request body");
                }
            });
        } else {
            HttpParameterLocation parameterLocation = HttpParameterLocation.valueOf(location.name());
            String name = parameterName.orElseThrow();
            HttpParameterPin parameter = parameters.stream().filter(candidate ->
                candidate.location() == parameterLocation && (location == Location.HEADER
                    ? candidate.name().equalsIgnoreCase(name) : candidate.name().equals(name)))
                .findFirst().orElseThrow(() -> new IllegalArgumentException("callback parameter is not declared"));
            if (!path.equals(List.of(parameter.sourcePath().split("\\.")))) {
                throw new IllegalArgumentException("callback parameter source path disagrees with its pin");
            }
            if (security.permits(parameterLocation, name) || idempotency.stream().anyMatch(target ->
                target.location() == parameterLocation && target.name().equalsIgnoreCase(name))) {
                throw new IllegalArgumentException("callback target collides with authorization or idempotency");
            }
        }
        JsonNode schema = requestSchema.node();
        for (String field : path) {
            if (!"object".equals(schema.path("type").asText()) || !schema.path("properties").has(field)) {
                throw new IllegalArgumentException("callback target must traverse declared object fields");
            }
            schema = schema.path("properties").path(field);
        }
        String format = schema.path("format").asText("");
        if (!"string".equals(schema.path("type").asText())
            || !(format.isEmpty() || "uri".equals(format) || "uri-reference".equals(format))) {
            throw new IllegalArgumentException("callback target must be a string/URI-compatible field");
        }
    }

    /** Copies the mapped request; any authored value, including JSON null, is rejected. */
    public JsonNode inject(JsonNode wire, ConnectorCallbackContext callback) {
        Objects.requireNonNull(callback, "callback context");
        if (!(Objects.requireNonNull(wire, "mapped HTTP request") instanceof ObjectNode object)) {
            throw new IllegalArgumentException("callback request must be an object");
        }
        ObjectNode copy = object.deepCopy();
        ObjectNode parent = copy;
        for (String field : path.subList(0, path.size() - 1)) {
            if (!parent.has(field)) {
                parent = parent.putObject(field);
            } else if (parent.get(field) instanceof ObjectNode child) {
                parent = child;
            } else {
                throw new IllegalArgumentException("callback target traverses a non-object value");
            }
        }
        String leaf = path.getLast();
        if (parent.has(leaf)) throw new IllegalArgumentException("mapped request contains reserved callback field");
        parent.put(leaf, callback.callbackUri().toASCIIString());
        return copy;
    }
}
