package org.pipelineframework.orchestrator;

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
    String bundleVersionId,
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
    public ExecutionRecord(
        String tenantId,
        String executionId,
        String executionKey,
        String pipelineId,
        String bundleVersionId,
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
            bundleVersionId,
            bundleVersionId,
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
            PipelineBundleManifest.DEFAULT_PIPELINE_ID,
            PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION,
            PipelineBundleManifest.DEFAULT_BUNDLE_VERSION_ID,
            PipelineBundleManifest.DEFAULT_BUNDLE_VERSION_ID,
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
            bundleVersionId,
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
            ttlEpochS);
    }
}
