package org.pipelineframework.orchestrator;

/**
 * Durable execution state record used by async orchestration.
 */
public record ExecutionRecord<I, R>(
    String tenantId,
    String executionId,
    String executionKey,
    ExecutionStatus status,
    long version,
    int currentStepIndex,
    int attempt,
    String leaseOwner,
    long leaseExpiresEpochMs,
    long nextDueEpochMs,
    String lastTransitionKey,
    I inputPayload,
    R resultPayload,
    String errorCode,
    String errorMessage,
    long createdAtEpochMs,
    long updatedAtEpochMs,
    long ttlEpochS
) {
    /**
     * Returns a copy with a new status and timestamp.
     *
     * @param newStatus status to apply
     * @param nowEpochMs update timestamp
     * @return updated record
     */
    public ExecutionRecord<I, R> withStatus(ExecutionStatus newStatus, long nowEpochMs) {
        return new ExecutionRecord<>(
            tenantId,
            executionId,
            executionKey,
            newStatus,
            version,
            currentStepIndex,
            attempt,
            leaseOwner,
            leaseExpiresEpochMs,
            nextDueEpochMs,
            lastTransitionKey,
            inputPayload,
            resultPayload,
            errorCode,
            errorMessage,
            createdAtEpochMs,
            nowEpochMs,
            ttlEpochS);
    }
}
