package org.pipelineframework.orchestrator;

import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import java.util.Objects;
import java.util.Optional;

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
    String contractVersion,
    String releaseVersion,
    Object inputPayload,
    ExecutionResultShape resultShape,
    Optional<String> inputCanonicalTypeId,
    int initialStepIndex,
    long nowEpochMs,
    long ttlEpochS
) {
    public ExecutionCreateCommand(
        String tenantId,
        String executionKey,
        String pipelineId,
        String contractVersion,
        String releaseVersion,
        Object inputPayload,
        ExecutionResultShape resultShape,
        long nowEpochMs,
        long ttlEpochS
    ) {
        this(
            tenantId,
            executionKey,
            pipelineId,
            contractVersion,
            releaseVersion,
            inputPayload,
            resultShape,
            Optional.empty(),
            0,
            nowEpochMs,
            ttlEpochS);
    }

    public ExecutionCreateCommand(
        String tenantId,
        String executionKey,
        String pipelineId,
        String contractVersion,
        String releaseVersion,
        Object inputPayload,
        ExecutionResultShape resultShape,
        int initialStepIndex,
        long nowEpochMs,
        long ttlEpochS
    ) {
        this(
            tenantId,
            executionKey,
            pipelineId,
            contractVersion,
            releaseVersion,
            inputPayload,
            resultShape,
            Optional.empty(),
            initialStepIndex,
            nowEpochMs,
            ttlEpochS);
    }

    public ExecutionCreateCommand(
        String tenantId,
        String executionKey,
        String pipelineId,
        String releaseVersion,
        Object inputPayload,
        ExecutionResultShape resultShape,
        long nowEpochMs,
        long ttlEpochS
    ) {
        this(
            tenantId,
            executionKey,
            pipelineId,
            PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION,
            releaseVersion,
            inputPayload,
            resultShape,
            Optional.empty(),
            0,
            nowEpochMs,
            ttlEpochS);
    }

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
            PipelineContractDescriptor.DEFAULT_PIPELINE_ID,
            PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION,
            PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION,
            inputPayload,
            resultShape,
            Optional.empty(),
            0,
            nowEpochMs,
            ttlEpochS);
    }

    public ExecutionCreateCommand {
        Objects.requireNonNull(tenantId, "ExecutionCreateCommand.tenantId must not be null");
        Objects.requireNonNull(executionKey, "ExecutionCreateCommand.executionKey must not be null");
        Objects.requireNonNull(pipelineId, "ExecutionCreateCommand.pipelineId must not be null");
        Objects.requireNonNull(contractVersion, "ExecutionCreateCommand.contractVersion must not be null");
        Objects.requireNonNull(releaseVersion, "ExecutionCreateCommand.releaseVersion must not be null");
        Objects.requireNonNull(resultShape, "ExecutionCreateCommand.resultShape must not be null");
        inputCanonicalTypeId = Objects.requireNonNull(inputCanonicalTypeId,
            "ExecutionCreateCommand.inputCanonicalTypeId must not be null");
        inputCanonicalTypeId = inputCanonicalTypeId.filter(value -> !value.isBlank());
        if (initialStepIndex < 0) {
            throw new IllegalArgumentException("ExecutionCreateCommand.initialStepIndex must be >= 0");
        }
    }
}
