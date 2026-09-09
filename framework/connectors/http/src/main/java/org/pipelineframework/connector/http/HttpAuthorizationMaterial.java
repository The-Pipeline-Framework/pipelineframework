package org.pipelineframework.connector.http;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Runtime-only authorization fields produced by the host-owned connection. */
public record HttpAuthorizationMaterial(
    Map<String, List<String>> headers,
    Map<String, List<String>> query,
    Map<String, List<String>> cookies
) {
    public HttpAuthorizationMaterial {
        headers = copy(headers, "authorization header", true);
        query = copy(query, "authorization query field", false);
        cookies = copy(cookies, "authorization cookie", false);
    }

    public static HttpAuthorizationMaterial none() {
        return new HttpAuthorizationMaterial(Map.of(), Map.of(), Map.of());
    }

    public void requireAllowedBy(HttpSecurityConstraint constraint) {
        Objects.requireNonNull(constraint, "HTTP security constraint must not be null");
        headers.keySet().forEach(name -> requireAllowed(constraint, HttpParameterLocation.HEADER, name));
        query.keySet().forEach(name -> requireAllowed(constraint, HttpParameterLocation.QUERY, name));
        cookies.keySet().forEach(name -> requireAllowed(constraint, HttpParameterLocation.COOKIE, name));
    }

    private static void requireAllowed(
        HttpSecurityConstraint constraint,
        HttpParameterLocation location,
        String name
    ) {
        if (!constraint.permits(location, name)) {
            throw new IllegalArgumentException("host authorization material targets an unselected HTTP field: "
                + location + ":" + name);
        }
    }

    private static Map<String, List<String>> copy(Map<String, List<String>> source, String subject, boolean header) {
        Objects.requireNonNull(source, subject + " values must not be null");
        if (source.size() > 32) throw new IllegalArgumentException(subject + " values exceed field limit");
        Map<String, List<String>> result = new LinkedHashMap<>();
        source.forEach((name, values) -> {
            String key = Objects.requireNonNull(name, subject + " name must not be null").trim();
            if (!key.matches("[!#$%&'*+.^_`|~0-9A-Za-z-]{1,128}")) {
                throw new IllegalArgumentException(subject + " has an invalid name: " + key);
            }
            if (header && switch (key.toLowerCase(Locale.ROOT)) {
                case "host", "content-length", "content-type", "cookie", "connection", "expect", "upgrade",
                    "transfer-encoding" -> true;
                default -> false;
            }) {
                throw new IllegalArgumentException(subject + " cannot set reserved header '" + key + "'");
            }
            List<String> copied = List.copyOf(Objects.requireNonNull(values, subject + " list must not be null"));
            if (copied.isEmpty() || copied.size() > 16 || copied.stream().anyMatch(value ->
                value == null || value.isEmpty() || value.length() > 16 * 1024
                    || value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0)) {
                throw new IllegalArgumentException(
                    subject + " values must be a bounded non-empty list without CR/LF");
            }
            if (result.putIfAbsent(key, copied) != null) {
                throw new IllegalArgumentException("duplicate " + subject + " name: " + key);
            }
        });
        return Collections.unmodifiableMap(result);
    }
}
