package org.pipelineframework.orchestrator.dto;

import java.util.stream.Stream;

import org.pipelineframework.orchestrator.SerializedTransitionPayload;

/**
 * Generic hosted-control-plane await completion request.
 *
 * @param interactionId interaction id to complete
 * @param correlationId external correlation id to complete
 * @param resumeToken signed resume token for webhook completion
 * @param idempotencyKey completion idempotency key
 * @param responsePayload serialized completion payload
 * @param actor actor admitting the completion
 */
public record HostedAwaitCompletionRequest(
    String interactionId,
    String correlationId,
    String resumeToken,
    String idempotencyKey,
    SerializedTransitionPayload responsePayload,
    String actor
) {
    public HostedAwaitCompletionRequest {
        long identifiers = Stream.of(interactionId, correlationId, resumeToken)
            .filter(value -> value != null && !value.isBlank())
            .count();
        if (identifiers == 0) {
            throw new IllegalArgumentException("interactionId, correlationId, or resumeToken is required");
        }
        if (responsePayload == null) {
            throw new IllegalArgumentException("responsePayload is required");
        }
    }
}
