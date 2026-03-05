package org.pipelineframework.orchestrator;

/**
 * Terminal failure payload for dead-letter publishing.
 *
 * @param tenantId tenant identifier
 * @param executionId execution identifier
 * @param transitionKey transition idempotency key
 * @param errorCode error code
 * @param errorMessage error message
 * @param createdAtEpochMs event creation timestamp
 */
public record DeadLetterEnvelope(
    String tenantId,
    String executionId,
    String transitionKey,
    String errorCode,
    String errorMessage,
    long createdAtEpochMs
) {
}
