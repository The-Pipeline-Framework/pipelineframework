package org.pipelineframework.orchestrator;

import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import java.util.Objects;

/**
 * Portable command sent across the transition-worker seam.
 *
 * @param tenantId tenant identifier
 * @param executionId execution identifier
 * @param pipelineId pipeline identifier
 * @param contractVersion semantic pipeline contract version
 * @param releaseVersion active release version
 * @param currentStepIndex step index where execution should continue
 * @param stopBeforeStepIndex optional exclusive stop step index; negative means run to pipeline end
 * @param attempt current execution attempt
 * @param resultShape expected materialized result shape
 * @param executionVersion claimed execution record version
 * @param transitionKey transition idempotency key
 * @param traceId trace/correlation identifier for this transition
 * @param payloadTypeId encoded input payload type id
 * @param payloadEncoding encoded input payload encoding
 * @param payload encoded input payload
 * @param redriveIntent explicit terminal-redrive intent
 * @param redriveStepIndex failed Command step targeted by deliberate retry, or {@code -1}
 */
public record TransitionCommandEnvelope(
    String tenantId,
    String executionId,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    int currentStepIndex,
    int stopBeforeStepIndex,
    int attempt,
    ExecutionResultShape resultShape,
    long executionVersion,
    String transitionKey,
    String traceId,
    String payloadTypeId,
    String payloadEncoding,
    String payload,
    ExecutionRedriveIntent redriveIntent,
    int redriveStepIndex
) {
    public TransitionCommandEnvelope(
        String tenantId,
        String executionId,
        String pipelineId,
        String contractVersion,
        String releaseVersion,
        int currentStepIndex,
        int stopBeforeStepIndex,
        int attempt,
        ExecutionResultShape resultShape,
        long executionVersion,
        String transitionKey,
        String traceId,
        String payloadTypeId,
        String payloadEncoding,
        String payload
    ) {
        this(tenantId, executionId, pipelineId, contractVersion, releaseVersion, currentStepIndex,
            stopBeforeStepIndex, attempt, resultShape, executionVersion, transitionKey, traceId,
            payloadTypeId, payloadEncoding, payload, ExecutionRedriveIntent.REPLAY, -1);
    }

    public TransitionCommandEnvelope(
        String tenantId,
        String executionId,
        String pipelineId,
        String releaseVersion,
        int currentStepIndex,
        int attempt,
        ExecutionResultShape resultShape,
        long executionVersion,
        String transitionKey,
        String traceId,
        String payloadTypeId,
        String payloadEncoding,
        String payload
    ) {
        this(
            tenantId,
            executionId,
            pipelineId,
            PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION,
            releaseVersion,
            currentStepIndex,
            -1,
            attempt,
            resultShape,
            executionVersion,
            transitionKey,
            traceId,
            payloadTypeId,
            payloadEncoding,
            payload,
            ExecutionRedriveIntent.REPLAY,
            -1);
    }

    public TransitionCommandEnvelope {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        contractVersion = contractVersion == null || contractVersion.isBlank()
            ? PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION
            : contractVersion;
        releaseVersion = releaseVersion == null || releaseVersion.isBlank()
            ? contractVersion
            : releaseVersion;
        if (currentStepIndex < 0) {
            throw new IllegalArgumentException("currentStepIndex must be >= 0");
        }
        if (stopBeforeStepIndex >= 0 && stopBeforeStepIndex < currentStepIndex) {
            throw new IllegalArgumentException("stopBeforeStepIndex must be greater than or equal to currentStepIndex when set");
        }
        if (attempt < 0) {
            throw new IllegalArgumentException("attempt must be >= 0");
        }
        Objects.requireNonNull(resultShape, "resultShape");
        if (executionVersion < 0) {
            throw new IllegalArgumentException("executionVersion must be >= 0");
        }
        if (transitionKey == null || transitionKey.isBlank()) {
            throw new IllegalArgumentException("transitionKey must not be blank");
        }
        Objects.requireNonNull(traceId, "traceId");
        Objects.requireNonNull(payloadTypeId, "payloadTypeId");
        Objects.requireNonNull(payloadEncoding, "payloadEncoding");
        Objects.requireNonNull(payload, "payload");
        redriveIntent = redriveIntent == null ? ExecutionRedriveIntent.REPLAY : redriveIntent;
        if (redriveIntent == ExecutionRedriveIntent.RETRY_FAILED_COMMAND && redriveStepIndex < currentStepIndex) {
            throw new IllegalArgumentException(
                "redriveStepIndex must identify a step at or after currentStepIndex for deliberate Command retry");
        }
        if (redriveIntent == ExecutionRedriveIntent.REPLAY) {
            redriveStepIndex = -1;
        }
    }

    public static TransitionCommandEnvelope from(
        TransitionWorkerCommand command,
        String pipelineId,
        String releaseVersion,
        String traceId,
        SerializedTransitionPayload encodedPayload
    ) {
        return from(
            command,
            pipelineId,
            PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION,
            releaseVersion,
            traceId,
            encodedPayload);
    }

    public static TransitionCommandEnvelope from(
        TransitionWorkerCommand command,
        String pipelineId,
        String contractVersion,
        String releaseVersion,
        String traceId,
        SerializedTransitionPayload encodedPayload
    ) {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(encodedPayload, "encodedPayload");
        return new TransitionCommandEnvelope(
            command.tenantId(),
            command.executionId(),
            pipelineId,
            contractVersion,
            releaseVersion,
            command.currentStepIndex(),
            command.stopBeforeStepIndex(),
            command.attempt(),
            command.resultShape(),
            command.executionVersion(),
            command.transitionKey(),
            traceId,
            encodedPayload.payloadTypeId(),
            encodedPayload.payloadEncoding(),
            encodedPayload.payload(),
            command.redriveIntent(),
            command.redriveStepIndex());
    }

    public TransitionWorkerCommand toCommand(TransitionPayloadCodec codec) {
        Objects.requireNonNull(codec, "codec");
        return new TransitionWorkerCommand(
            tenantId,
            executionId,
            currentStepIndex,
            stopBeforeStepIndex,
            attempt,
            resultShape,
            executionVersion,
            transitionKey,
            codec.decode(serializedPayload()),
            redriveIntent,
            redriveStepIndex);
    }

    public SerializedTransitionPayload serializedPayload() {
        return new SerializedTransitionPayload(payloadTypeId, payloadEncoding, payload);
    }
}
