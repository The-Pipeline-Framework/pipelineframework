package org.pipelineframework.orchestrator;

import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
/**
 * Durable execution state record used by async orchestration.
 */
public record ExecutionRecord<I, R>(
    String tenantId,
    String executionId,
    String executionKey,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    ExecutionResultShape resultShape,
    ExecutionStatus status,
    long version,
    int currentStepIndex,
    int attempt,
    String leaseOwner,
    long leaseExpiresEpochMs,
    long nextDueEpochMs,
    String lastTransitionKey,
    I inputPayload,
    String awaitUnitId,
    R resultPayload,
    String errorCode,
    String errorMessage,
    long createdAtEpochMs,
    long updatedAtEpochMs,
    long ttlEpochS,
    long firstCircuitDeferredAtEpochMs,
    int circuitDeferralCount,
    String circuitIdentity
) {
    /** Compatibility constructor for records persisted before circuit deferral metadata existed. */
    public ExecutionRecord(
        String tenantId,
        String executionId,
        String executionKey,
        String pipelineId,
        String contractVersion,
        String releaseVersion,
        ExecutionResultShape resultShape,
        ExecutionStatus status,
        long version,
        int currentStepIndex,
        int attempt,
        String leaseOwner,
        long leaseExpiresEpochMs,
        long nextDueEpochMs,
        String lastTransitionKey,
        I inputPayload,
        String awaitUnitId,
        R resultPayload,
        String errorCode,
        String errorMessage,
        long createdAtEpochMs,
        long updatedAtEpochMs,
        long ttlEpochS
    ) {
        this(tenantId, executionId, executionKey, pipelineId, contractVersion, releaseVersion, resultShape,
            status, version, currentStepIndex, attempt, leaseOwner, leaseExpiresEpochMs, nextDueEpochMs,
            lastTransitionKey, inputPayload, awaitUnitId, resultPayload, errorCode, errorMessage,
            createdAtEpochMs, updatedAtEpochMs, ttlEpochS, 0L, 0, "");
    }
    public ExecutionRecord(
        String tenantId,
        String executionId,
        String executionKey,
        String pipelineId,
        String releaseVersion,
        ExecutionResultShape resultShape,
        ExecutionStatus status,
        long version,
        int currentStepIndex,
        int attempt,
        String leaseOwner,
        long leaseExpiresEpochMs,
        long nextDueEpochMs,
        String lastTransitionKey,
        I inputPayload,
        String awaitUnitId,
        R resultPayload,
        String errorCode,
        String errorMessage,
        long createdAtEpochMs,
        long updatedAtEpochMs,
        long ttlEpochS
    ) {
        this(
            tenantId,
            executionId,
            executionKey,
            pipelineId,
            PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION,
            releaseVersion,
            resultShape,
            status,
            version,
            currentStepIndex,
            attempt,
            leaseOwner,
            leaseExpiresEpochMs,
            nextDueEpochMs,
            lastTransitionKey,
            inputPayload,
            awaitUnitId,
            resultPayload,
            errorCode,
            errorMessage,
            createdAtEpochMs,
            updatedAtEpochMs,
            ttlEpochS);
    }

    public ExecutionRecord(
        String tenantId,
        String executionId,
        String executionKey,
        ExecutionResultShape resultShape,
        ExecutionStatus status,
        long version,
        int currentStepIndex,
        int attempt,
        String leaseOwner,
        long leaseExpiresEpochMs,
        long nextDueEpochMs,
        String lastTransitionKey,
        I inputPayload,
        String awaitUnitId,
        R resultPayload,
        String errorCode,
        String errorMessage,
        long createdAtEpochMs,
        long updatedAtEpochMs,
        long ttlEpochS
    ) {
        this(
            tenantId,
            executionId,
            executionKey,
            PipelineContractDescriptor.DEFAULT_PIPELINE_ID,
            PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION,
            PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION,
            resultShape,
            status,
            version,
            currentStepIndex,
            attempt,
            leaseOwner,
            leaseExpiresEpochMs,
            nextDueEpochMs,
            lastTransitionKey,
            inputPayload,
            awaitUnitId,
            resultPayload,
            errorCode,
            errorMessage,
            createdAtEpochMs,
            updatedAtEpochMs,
            ttlEpochS);
    }

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
            pipelineId,
            contractVersion,
            releaseVersion,
            resultShape,
            newStatus,
            version,
            currentStepIndex,
            attempt,
            leaseOwner,
            leaseExpiresEpochMs,
            nextDueEpochMs,
            lastTransitionKey,
            inputPayload,
            awaitUnitId,
            resultPayload,
            errorCode,
            errorMessage,
            createdAtEpochMs,
            nowEpochMs,
            ttlEpochS,
            firstCircuitDeferredAtEpochMs,
            circuitDeferralCount,
            circuitIdentity);
    }
}
