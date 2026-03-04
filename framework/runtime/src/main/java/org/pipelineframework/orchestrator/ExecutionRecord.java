package org.pipelineframework.orchestrator;

/**
 * Durable execution state record used by async orchestration.
 */
public record ExecutionRecord(
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
    Object inputPayload,
    Object resultPayload,
    String errorCode,
    String errorMessage,
    long createdAtEpochMs,
    long updatedAtEpochMs,
    long ttlEpochS
) {
    /**
     * Create a copy of this record with an updated status and update timestamp.
     *
     * @param newStatus the status to set on the returned record
     * @param nowEpochMs the value (epoch milliseconds) to set for `updatedAtEpochMs`
     * @return a copy of this ExecutionRecord with `status` = `newStatus` and `updatedAtEpochMs` = `nowEpochMs`
     */
    public ExecutionRecord withStatus(ExecutionStatus newStatus, long nowEpochMs) {
        return new ExecutionRecord(
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
