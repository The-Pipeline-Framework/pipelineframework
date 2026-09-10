package org.pipelineframework.blocks.openapi.mapping;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.fasterxml.jackson.databind.JsonNode;
import org.pipelineframework.config.pipeline.PipelineJson;

final class OpenApiMappingJson {
    private OpenApiMappingJson() {
    }

    static JsonNode schema(String json, String label) {
        try {
            JsonNode value = PipelineJson.mapper().readTree(require(json, label));
            if (value == null || !value.isObject()) {
                throw new IllegalArgumentException(label + " must be a JSON object schema");
            }
            return value;
        } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
            throw new IllegalArgumentException(label + " must be valid JSON", failure);
        }
    }

    static String fingerprint(String json, String label) {
        try {
            byte[] canonical = PipelineJson.mapper().writeValueAsBytes(schema(json, label));
            return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(canonical));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
            throw new IllegalArgumentException(label + " cannot be canonicalized", failure);
        }
    }

    static String require(String value, String label) {
        String checked = java.util.Objects.requireNonNull(value, label + " must not be null").trim();
        if (checked.isEmpty()) throw new IllegalArgumentException(label + " must not be blank");
        return checked;
    }
}
