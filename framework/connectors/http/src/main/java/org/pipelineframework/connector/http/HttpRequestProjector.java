package org.pipelineframework.connector.http;

import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import org.pipelineframework.connector.CommandDispatchIdentity;

/** Deterministic, bounded projection from a canonical input into one pinned HTTP request. */
final class HttpRequestProjector {
    private HttpRequestProjector() {
    }

    static HttpRequest project(
        HttpOperationPin pin,
        JsonNode wireInput,
        HttpClientConnection connection,
        HttpAuthorizationMaterial authorization,
        HttpProviderConfiguration configuration,
        Optional<CommandDispatchIdentity> dispatchIdentity
    ) {
        return project(pin, wireInput, connection, authorization, configuration, dispatchIdentity, Optional.empty());
    }

    static HttpRequest project(HttpOperationPin pin, JsonNode wireInput, HttpClientConnection connection,
        HttpAuthorizationMaterial authorization, HttpProviderConfiguration configuration,
        Optional<CommandDispatchIdentity> dispatchIdentity,
        Optional<org.pipelineframework.connector.ConnectorCallbackContext> callbackContext) {
        var selectedCallback = pin.selectCallback(callbackContext);
        if (selectedCallback.isPresent() && !"https".equalsIgnoreCase(connection.baseUri().getScheme())
            && callbackContext.orElseThrow().uriPolicy()
                != org.pipelineframework.connector.ConnectorCallbackContext.UriPolicy.LOCAL_HTTP) {
            throw new IllegalArgumentException("HTTP callback tokens require an HTTPS provider endpoint or explicit local HTTP policy");
        }
        JsonNode requestInput = selectedCallback
            .map(callback -> callback.target().inject(wireInput, callbackContext.orElseThrow()))
            .orElse(wireInput);
        HttpWireValueValidator.validate(requestInput, pin.requestSchema());
        String path = pin.relativePathTemplate();
        List<QueryValue> query = new ArrayList<>();
        Map<String, List<String>> headers = new LinkedHashMap<>();
        List<QueryValue> cookies = new ArrayList<>();
        for (HttpParameterPin parameter : pin.parameters()) {
            Optional<JsonNode> selected = select(requestInput, parameter.sourcePath());
            if (selected.isEmpty() || selected.orElseThrow().isNull()) {
                if (parameter.required()) {
                    throw new IllegalArgumentException("required HTTP parameter is absent: " + parameter.name());
                }
                continue;
            }
            JsonNode value = selected.orElseThrow();
            HttpWireValueValidator.validate(value, parameter.schema());
            switch (parameter.location()) {
                case PATH -> path = path.replace("{" + parameter.name() + "}", pathValue(parameter, value));
                case QUERY -> query.addAll(queryValues(parameter, value));
                case HEADER -> putHeader(headers, parameter.name(), List.of(simple(value, parameter.explode())));
                case COOKIE -> cookies.addAll(formValues(parameter.name(), value, parameter.explode(), false));
            }
        }
        mergeAuthorization(query, headers, cookies, authorization);
        pin.providerIdempotencyKey().ifPresent(target -> {
            CommandDispatchIdentity identity = dispatchIdentity.orElseThrow(() ->
                new IllegalStateException("provider idempotency-key projection requires Command dispatch identity"));
            putHeader(headers, target.name(), List.of(identity.providerIdempotencyKey()));
        });
        String queryString = query.stream().map(value -> encode(value.name(), false) + "="
            + encode(value.value(), value.allowReserved())).reduce((left, right) -> left + "&" + right).orElse("");
        URI uri = requestUri(connection.baseUri(), path, queryString);
        String cookieHeader = cookies.stream().map(value -> encode(value.name(), false) + "="
            + encode(value.value(), false)).reduce((left, right) -> left + "; " + right).orElse("");
        byte[] body = pin.requestBody().map(requestBody -> body(requestBody, requestInput)).orElseGet(() -> new byte[0]);
        if (!cookieHeader.isEmpty()) putHeader(headers, "Cookie", List.of(cookieHeader));
        if (body.length > 0) pin.requestBody().ifPresent(value ->
            putHeader(headers, "Content-Type", List.of(value.mediaType())));
        if (requestBytes(uri, headers, body) > configuration.effectiveMaxRequestBytes()) {
            throw new IllegalArgumentException("pinned HTTP request exceeds configured size limit");
        }
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri).timeout(configuration.effectiveRequestTimeout());
        headers.forEach((name, values) -> values.forEach(value -> builder.header(name, value)));
        return builder.method(pin.method(), body.length == 0
            ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(body)).build();
    }

    private static long requestBytes(
        URI uri,
        Map<String, List<String>> headers,
        byte[] body
    ) {
        long size = uri.toASCIIString().getBytes(StandardCharsets.UTF_8).length + body.length;
        for (Map.Entry<String, List<String>> header : headers.entrySet()) {
            size += header.getKey().getBytes(StandardCharsets.UTF_8).length;
            for (String value : header.getValue()) size += value.getBytes(StandardCharsets.UTF_8).length;
        }
        return size;
    }

    private static byte[] body(HttpRequestBodyPin pin, JsonNode wireInput) {
        Optional<JsonNode> selected = pin.sourcePath().flatMap(path -> select(wireInput, path));
        Optional<JsonNode> value = pin.sourcePath().isPresent() ? selected : Optional.of(wireInput);
        if (value.isEmpty() || value.orElseThrow().isNull()) {
            if (pin.required()) throw new IllegalArgumentException("required HTTP request body is absent");
            return new byte[0];
        }
        HttpWireValueValidator.validate(value.orElseThrow(), pin.schema());
        return value.orElseThrow().toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void mergeAuthorization(
        List<QueryValue> query,
        Map<String, List<String>> headers,
        List<QueryValue> cookies,
        HttpAuthorizationMaterial authorization
    ) {
        authorization.headers().forEach((name, values) -> putHeader(headers, name, values));
        for (Map.Entry<String, List<String>> entry : authorization.query().entrySet()) {
            if (query.stream().anyMatch(value -> value.name().equals(entry.getKey()))) {
                throw new IllegalArgumentException("authorization query field collides with operation input: " + entry.getKey());
            }
            entry.getValue().forEach(value -> query.add(new QueryValue(entry.getKey(), value, false)));
        }
        for (Map.Entry<String, List<String>> entry : authorization.cookies().entrySet()) {
            if (cookies.stream().anyMatch(value -> value.name().equals(entry.getKey()))) {
                throw new IllegalArgumentException("authorization cookie collides with operation input: " + entry.getKey());
            }
            entry.getValue().forEach(value -> cookies.add(new QueryValue(entry.getKey(), value, false)));
        }
    }

    private static void putHeader(Map<String, List<String>> headers, String name, List<String> values) {
        String collision = headers.keySet().stream().filter(existing -> existing.equalsIgnoreCase(name)).findFirst().orElse("");
        if (!collision.isEmpty()) throw new IllegalArgumentException("HTTP header collision: " + name);
        headers.put(name, values);
    }

    private static URI requestUri(URI baseUri, String path, String query) {
        String base = baseUri.toString();
        while (base.endsWith("/")) base = base.substring(0, base.length() - 1);
        URI result = URI.create(base + path + (query.isEmpty() ? "" : "?" + query)).normalize();
        if (!sameOrigin(baseUri, result) || result.getPath().contains("/../") || result.getPath().endsWith("/..")) {
            throw new IllegalArgumentException("pinned HTTP path escaped the application-owned base URI");
        }
        return result;
    }

    static boolean sameOrigin(URI left, URI right) {
        String leftScheme = left.getScheme();
        String rightScheme = right.getScheme();
        String leftHost = left.getHost();
        String rightHost = right.getHost();
        if (leftScheme == null || rightScheme == null || leftHost == null || rightHost == null) return false;
        return leftScheme.equalsIgnoreCase(rightScheme)
            && leftHost.equalsIgnoreCase(rightHost)
            && effectivePort(left) == effectivePort(right);
    }

    private static int effectivePort(URI uri) {
        if (uri.getPort() >= 0) return uri.getPort();
        return "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
    }

    private static List<QueryValue> queryValues(HttpParameterPin parameter, JsonNode value) {
        return switch (parameter.style()) {
            case FORM -> formValues(parameter.name(), value, parameter.explode(), parameter.allowReserved());
            case SPACE_DELIMITED -> delimited(parameter.name(), value, " ", parameter.allowReserved());
            case PIPE_DELIMITED -> delimited(parameter.name(), value, "|", parameter.allowReserved());
            case DEEP_OBJECT -> deepObject(parameter.name(), value, parameter.allowReserved());
            default -> throw new IllegalArgumentException("unsupported query parameter style: " + parameter.style());
        };
    }

    private static String pathValue(HttpParameterPin parameter, JsonNode value) {
        return switch (parameter.style()) {
            case SIMPLE -> pathSimple(value, parameter.explode(), ",");
            case LABEL -> "." + pathSimple(value, parameter.explode(), parameter.explode() ? "." : ",");
            case MATRIX -> matrix(parameter.name(), value, parameter.explode());
            default -> throw new IllegalArgumentException("unsupported path parameter style: " + parameter.style());
        };
    }

    private static String pathSimple(JsonNode value, boolean explode, String separator) {
        if (value.isArray()) {
            List<String> values = new ArrayList<>();
            value.forEach(item -> values.add(pathScalar(item)));
            return String.join(separator, values);
        }
        if (value.isObject()) {
            return fields(value).stream().map(entry -> encode(entry.getKey(), false)
                    + (explode ? "=" : separator) + pathScalar(entry.getValue()))
                .reduce((left, right) -> left + separator + right).orElse("");
        }
        return pathScalar(value);
    }

    private static String matrix(String name, JsonNode value, boolean explode) {
        String encodedName = encode(name, false);
        if (value.isArray() && explode) {
            List<String> values = new ArrayList<>();
            value.forEach(item -> values.add(";" + encodedName + "=" + pathScalar(item)));
            return String.join("", values);
        }
        if (value.isObject() && explode) {
            return fields(value).stream().map(entry -> ";" + encode(entry.getKey(), false) + "="
                    + pathScalar(entry.getValue()))
                .reduce(String::concat).orElse("");
        }
        return ";" + encodedName + "=" + pathSimple(value, false, ",");
    }

    private static String pathScalar(JsonNode value) {
        String scalar = scalar(value);
        if (".".equals(scalar) || "..".equals(scalar)) {
            throw new IllegalArgumentException("HTTP path parameter cannot create a traversal segment");
        }
        return encode(scalar, false);
    }

    private static List<QueryValue> formValues(String name, JsonNode value, boolean explode, boolean allowReserved) {
        if (value.isArray()) {
            if (explode) {
                List<QueryValue> values = new ArrayList<>();
                value.forEach(item -> values.add(new QueryValue(name, scalar(item), allowReserved)));
                return values;
            }
            return List.of(new QueryValue(name, join(value, ","), allowReserved));
        }
        if (value.isObject()) {
            List<Map.Entry<String, JsonNode>> fields = fields(value);
            if (explode) {
                return fields.stream().map(entry -> new QueryValue(entry.getKey(), scalar(entry.getValue()), allowReserved)).toList();
            }
            return List.of(new QueryValue(name, fields.stream().map(entry -> entry.getKey() + "," + scalar(entry.getValue()))
                .reduce((left, right) -> left + "," + right).orElse(""), allowReserved));
        }
        return List.of(new QueryValue(name, scalar(value), allowReserved));
    }

    private static List<QueryValue> delimited(String name, JsonNode value, String delimiter, boolean allowReserved) {
        if (!value.isArray()) throw new IllegalArgumentException("delimited query parameters require an array");
        return List.of(new QueryValue(name, join(value, delimiter), allowReserved));
    }

    private static List<QueryValue> deepObject(String name, JsonNode value, boolean allowReserved) {
        if (!value.isObject()) throw new IllegalArgumentException("deepObject query parameters require an object");
        return fields(value).stream().map(entry ->
            new QueryValue(name + "[" + entry.getKey() + "]", scalar(entry.getValue()), allowReserved)).toList();
    }

    private static String simple(JsonNode value, boolean explode) {
        if (value.isArray()) return join(value, ",");
        if (value.isObject()) return fields(value).stream().map(entry -> entry.getKey()
            + (explode ? "=" : ",") + scalar(entry.getValue()))
            .reduce((left, right) -> left + "," + right).orElse("");
        return scalar(value);
    }

    private static String join(JsonNode array, String delimiter) {
        List<String> values = new ArrayList<>();
        array.forEach(item -> values.add(scalar(item)));
        return String.join(delimiter, values);
    }

    private static List<Map.Entry<String, JsonNode>> fields(JsonNode object) {
        List<Map.Entry<String, JsonNode>> fields = new ArrayList<>();
        object.fields().forEachRemaining(fields::add);
        fields.sort(Comparator.comparing(Map.Entry::getKey));
        return fields;
    }

    private static String scalar(JsonNode value) {
        if (value.isTextual()) return value.textValue();
        if (value.isNumber() || value.isBoolean()) return value.asText();
        throw new IllegalArgumentException("HTTP parameter values must match their pinned scalar/array/object shape");
    }

    private static Optional<JsonNode> select(JsonNode value, String path) {
        JsonNode current = Objects.requireNonNull(value, "HTTP input must not be null");
        for (String field : path.split("\\.")) {
            if (!current.isObject() || !current.has(field)) return Optional.empty();
            current = current.get(field);
        }
        return Optional.of(current);
    }

    private static String encode(String value, boolean allowReserved) {
        StringBuilder encoded = new StringBuilder();
        for (byte item : value.getBytes(StandardCharsets.UTF_8)) {
            int unsigned = item & 0xff;
            char character = (char) unsigned;
            boolean unreserved = character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z'
                || character >= '0' && character <= '9' || "-._~".indexOf(character) >= 0;
            boolean permittedReserved = allowReserved && ":/?@!$'()*+,;".indexOf(character) >= 0;
            if (unreserved || permittedReserved) encoded.append(character);
            else encoded.append('%').append(String.format(Locale.ROOT, "%02X", unsigned));
        }
        return encoded.toString();
    }

    private record QueryValue(String name, String value, boolean allowReserved) {
    }
}
