package org.pipelineframework.orchestrator;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Release-bound encoded canonical value stored at a durable runtime boundary.
 */
public record TypedDurablePayload(
    String canonicalTypeId,
    String typeExpressionFingerprint,
    String catalogFingerprint,
    String encoding,
    int encodingVersion,
    byte[] payload
) {
    public TypedDurablePayload {
        require(canonicalTypeId, "canonicalTypeId");
        require(typeExpressionFingerprint, "typeExpressionFingerprint");
        require(catalogFingerprint, "catalogFingerprint");
        require(encoding, "encoding");
        if (encodingVersion <= 0) {
            throw new IllegalArgumentException("encodingVersion must be positive");
        }
        payload = Objects.requireNonNull(payload, "payload").clone();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    /** Normalizes the durable JSON-map representation without accepting arbitrary maps as values. */
    public static Optional<TypedDurablePayload> fromDurableValue(Object value) {
        if (value instanceof TypedDurablePayload payload) {
            return Optional.of(payload);
        }
        if (!(value instanceof Map<?, ?> map)) {
            return Optional.empty();
        }
        Object version = map.get("encodingVersion");
        if (!(version instanceof Number number)) {
            return Optional.empty();
        }
        try {
            return Optional.of(new TypedDurablePayload(
                string(map, "canonicalTypeId"),
                string(map, "typeExpressionFingerprint"),
                string(map, "catalogFingerprint"),
                string(map, "encoding"),
                number.intValue(),
                java.util.Base64.getDecoder().decode(string(map, "payload"))));
        } catch (IllegalArgumentException ignored) {
            return Optional.empty();
        }
    }

    /** Reconstructs an envelope stored as its exact JSON bytes. */
    public static Optional<TypedDurablePayload> fromSerializedBytes(byte[] bytes) {
        try {
            return fromDurableValue(org.pipelineframework.config.pipeline.PipelineJson.mapper().readValue(bytes, Object.class));
        } catch (Exception ignored) {
            return Optional.empty();
        }
    }

    private static String string(Map<?, ?> map, String key) {
        Object value = map.get(key);
        if (!(value instanceof String string)) {
            throw new IllegalArgumentException("Typed durable payload field '" + key + "' must be a string");
        }
        return string;
    }

    private static void require(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
