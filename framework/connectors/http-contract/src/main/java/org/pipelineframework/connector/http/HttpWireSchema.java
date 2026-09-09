package org.pipelineframework.connector.http;

import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;

/** Normalized bounded wire schema retained without its source OpenAPI document. */
public record HttpWireSchema(String canonicalJson, String sha256) {
    public HttpWireSchema(String canonicalJson) {
        this(normalize(canonicalJson), HttpPinnedJson.sha256(normalize(canonicalJson)));
    }

    public HttpWireSchema {
        canonicalJson = normalize(canonicalJson);
        sha256 = requireText(sha256, "wire schema fingerprint");
        if (!HttpPinnedJson.sha256(canonicalJson).equals(sha256)) {
            throw new IllegalArgumentException("wire schema fingerprint mismatch");
        }
    }

    public JsonNode node() {
        return HttpPinnedJson.parse(canonicalJson);
    }

    private static String normalize(String value) {
        JsonNode parsed = HttpPinnedJson.parse(requireText(value, "wire schema"));
        if (!parsed.isObject() && !parsed.isBoolean()) {
            throw new IllegalArgumentException("wire schema must be a JSON Schema object or boolean");
        }
        return HttpPinnedJson.canonicalize(parsed);
    }

    private static String requireText(String value, String subject) {
        String result = Objects.requireNonNull(value, subject + " must not be null").trim();
        if (result.isEmpty()) throw new IllegalArgumentException(subject + " must not be blank");
        return result;
    }
}
