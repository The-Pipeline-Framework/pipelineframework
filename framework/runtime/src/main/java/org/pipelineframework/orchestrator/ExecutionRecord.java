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
     * Create a new ExecutionRecord with the given status and updated timestamp.
     *
     * All other fields are preserved from the current record; the current instance is not modified.
     *
     * @param newStatus the status to set on the new record
     * @param nowEpochMs the value to set for `updatedAtEpochMs`, in epoch milliseconds
     * @return an ExecutionRecord whose `status` is `newStatus` and whose `updatedAtEpochMs` is `nowEpochMs`, with all other fields unchanged
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
