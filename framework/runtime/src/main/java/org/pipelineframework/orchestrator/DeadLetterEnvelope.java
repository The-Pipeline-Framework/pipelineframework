package org.pipelineframework.orchestrator;

/**
 * Terminal failure payload for dead-letter publishing.
 *
 * @param tenantId tenant identifier
 * @param executionId execution identifier
 * @param executionKey deterministic execution key (idempotency/correlation)
 * @param correlationId correlation identifier for cross-system traceability
 * @param transitionKey transition idempotency key
 * @param resourceType failure resource type (for example orchestrator execution)
 * @param resourceName failure resource name (for example service/method)
 * @param transport transport path identifier (REST/GRPC/FUNCTION/LOCAL)
 * @param platform platform identifier (COMPUTE/FUNCTION)
 * @param terminalStatus terminal execution status when moved to DLQ
 * @param terminalReason terminal classification (retry_exhausted/non_retryable)
 * @param errorCode error code
 * @param errorMessage error message
 * @param retryable whether the originating failure class is retryable
 * @param retriesObserved retries observed before terminal transition
 * @param createdAtEpochMs event creation timestamp
 */
public record DeadLetterEnvelope(
    String tenantId,
    String executionId,
    String executionKey,
    String correlationId,
    String transitionKey,
    String resourceType,
    String resourceName,
    String transport,
    String platform,
    String terminalStatus,
    String terminalReason,
    String errorCode,
    String errorMessage,
    boolean retryable,
    int retriesObserved,
    long createdAtEpochMs
) {
}
