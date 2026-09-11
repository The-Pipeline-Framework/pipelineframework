package org.pipelineframework.connector;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Bounded raw material and release-selected security compatibility; contains no credentials from pins. */
public record ProviderCallbackAuthenticationRequest(ProviderCallbackRequest callback, String method,
    Map<String, List<String>> headers, byte[] body, List<SecurityRequirement> security) {
    public ProviderCallbackAuthenticationRequest {
        Objects.requireNonNull(callback, "callback");
        Objects.requireNonNull(method, "method");
        Objects.requireNonNull(headers, "headers");
        Objects.requireNonNull(body, "body");
        if (!method.equals("POST") || body.length > 1_048_576 || headers.size() > 64) {
            throw new IllegalArgumentException("callback request exceeds supported bounds");
        }
        headers = boundedHeaders(headers);
        body = body.clone();
        security = List.copyOf(security);
    }

    /** Validates raw header bounds before a transport performs durable lookup. */
    public static Map<String, List<String>> boundedHeaders(Map<String, List<String>> headers) {
        Objects.requireNonNull(headers, "headers");
        if (headers.size() > 64) {
            throw new IllegalArgumentException("callback headers exceed supported bounds");
        }
        var copied = new TreeMap<String, List<String>>(String.CASE_INSENSITIVE_ORDER);
        int headerBytes = 0;
        for (var entry : headers.entrySet()) {
            String name = Objects.requireNonNull(entry.getKey(), "header name");
            var values = List.copyOf(entry.getValue());
            if (!name.matches("[!#$%&'*+.^_`|~0-9A-Za-z-]{1,256}") || values.size() > 32
                || copied.containsKey(name)) {
                throw new IllegalArgumentException("invalid callback headers");
            }
            headerBytes += (name.length() + 4) * Math.max(1, values.size());
            for (String value : values) {
                if (value.length() > 8192 || value.chars().anyMatch(c -> c == 0 || c == '\r' || c == '\n')) {
                    throw new IllegalArgumentException("invalid callback header value");
                }
                headerBytes += value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            }
            copied.put(name, values);
        }
        if (headerBytes > 32768) {
            throw new IllegalArgumentException("callback headers exceed supported bounds");
        }
        return java.util.Collections.unmodifiableMap(copied);
    }

    @Override
    public byte[] body() { return body.clone(); }

    @Override
    public String toString() { return "ProviderCallbackAuthenticationRequest[redacted]"; }

    public record SecurityRequirement(String scheme, List<String> scopes, List<SecurityTarget> targets) {
        public SecurityRequirement {
            Objects.requireNonNull(scheme, "security scheme");
            if (scheme.isBlank() || scheme.length() > 256 || scheme.chars().anyMatch(Character::isISOControl)) {
                throw new IllegalArgumentException("invalid callback security scheme");
            }
            scopes = List.copyOf(scopes);
            targets = List.copyOf(targets);
        }
    }

    public record SecurityTarget(String location, String name) {
        public SecurityTarget {
            Objects.requireNonNull(location, "location");
            Objects.requireNonNull(name, "name");
            if (!List.of("HEADER", "QUERY", "COOKIE").contains(location) || name.isBlank() || name.length() > 256) {
                throw new IllegalArgumentException("invalid callback security target");
            }
        }
    }
}
