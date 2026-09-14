package org.pipelineframework.orchestrator;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Serialized payload carried across the transition-worker seam.
 *
 * @param payloadTypeId stable Java/runtime type identifier for the payload
 * @param payloadEncoding encoding used for {@code payload}
 * @param payload serialized payload body
 */
public record SerializedTransitionPayload(
    String payloadTypeId,
    String payloadEncoding,
    String payload) {
    public SerializedTransitionPayload {
        Objects.requireNonNull(payloadTypeId, "SerializedTransitionPayload.payloadTypeId must not be null");
        Objects.requireNonNull(payloadEncoding, "SerializedTransitionPayload.payloadEncoding must not be null");
        Objects.requireNonNull(payload, "SerializedTransitionPayload.payload must not be null");
    }

    /**
     * Reconstructs the envelope from the generic map shape returned by durable JSON stores.
     *
     * @param value a live envelope or its durable map representation
     * @return the normalized envelope when the value has all required envelope fields
     */
    public static Optional<SerializedTransitionPayload> fromDurableValue(Object value) {
        if (value instanceof SerializedTransitionPayload serialized) {
            return Optional.of(serialized);
        }
        if (!(value instanceof Map<?, ?> map)) {
            return Optional.empty();
        }
        Object typeId = map.get("payloadTypeId");
        Object encoding = map.get("payloadEncoding");
        Object payload = map.get("payload");
        if (typeId instanceof String type && !type.isBlank()
            && encoding instanceof String format && !format.isBlank()
            && payload instanceof String body) {
            return Optional.of(new SerializedTransitionPayload(type, format, body));
        }
        return Optional.empty();
    }
}
