package org.pipelineframework.orchestrator;

import java.util.Objects;

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
}
