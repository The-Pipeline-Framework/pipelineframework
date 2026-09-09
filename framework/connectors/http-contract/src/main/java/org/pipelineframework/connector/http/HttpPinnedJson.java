package org.pipelineframework.connector.http;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/** Canonical JSON and digest support for release-pinned HTTP metadata. */
public final class HttpPinnedJson {
    public static final int MAX_RESOURCE_BYTES = 4 * 1024 * 1024;
    private static final ObjectMapper JSON = new ObjectMapper()
        .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

    private HttpPinnedJson() {
    }

    public static JsonNode parse(String value) {
        try {
            return JSON.readTree(value);
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("invalid pinned HTTP JSON", failure);
        }
    }

    public static String canonicalize(JsonNode value) {
        try {
            return JSON.writeValueAsString(sorted(value));
        } catch (JsonProcessingException failure) {
            throw new IllegalArgumentException("unable to canonicalize pinned HTTP JSON", failure);
        }
    }

    private static JsonNode sorted(JsonNode value) {
        if (value.isObject()) {
            var result = JSON.createObjectNode();
            java.util.TreeMap<String, JsonNode> fields = new java.util.TreeMap<>();
            value.fields().forEachRemaining(entry -> fields.put(entry.getKey(), entry.getValue()));
            fields.forEach((name, child) -> result.set(name, sorted(child)));
            return result;
        }
        if (value.isArray()) {
            var result = JSON.createArrayNode();
            value.forEach(child -> result.add(sorted(child)));
            return result;
        }
        return value.deepCopy();
    }

    public static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
