package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Input command for creating or resolving an async execution.
 *
 * @param tenantId tenant identifier
 * @param executionKey deduplication key for this submission
 * @param inputPayload original orchestrator input payload
 * @param nowEpochMs current timestamp in epoch milliseconds
 * @param ttlEpochS expiration timestamp in epoch seconds
 */
public record ExecutionCreateCommand(
    String tenantId,
    String executionKey,
    String pipelineId,
    String bundleVersionId,
    Object inputPayload,
    ExecutionResultShape resultShape,
    long nowEpochMs,
    long ttlEpochS
) {
    public ExecutionCreateCommand(
        String tenantId,
        String executionKey,
        Object inputPayload,
        ExecutionResultShape resultShape,
        long nowEpochMs,
        long ttlEpochS
    ) {
        this(
            tenantId,
            executionKey,
            PipelineBundleManifest.DEFAULT_PIPELINE_ID,
            PipelineBundleManifest.DEFAULT_BUNDLE_VERSION_ID,
            inputPayload,
            resultShape,
            nowEpochMs,
            ttlEpochS);
    }

    public ExecutionCreateCommand {
        Objects.requireNonNull(tenantId, "ExecutionCreateCommand.tenantId must not be null");
        Objects.requireNonNull(executionKey, "ExecutionCreateCommand.executionKey must not be null");
        Objects.requireNonNull(pipelineId, "ExecutionCreateCommand.pipelineId must not be null");
        Objects.requireNonNull(bundleVersionId, "ExecutionCreateCommand.bundleVersionId must not be null");
        Objects.requireNonNull(resultShape, "ExecutionCreateCommand.resultShape must not be null");
    }
}
