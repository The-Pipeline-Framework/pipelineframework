package org.pipelineframework.execution;

import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;

/** Framework-managed execution scope for steps that need durable pipeline identity. */
public record PipelineExecutionContext(
    String tenantId,
    String executionId,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    int currentStepIndex,
    Optional<String> correlationId,
    Optional<String> traceId
) {
    public PipelineExecutionContext(String tenantId, String executionId, int currentStepIndex) {
        this(
            tenantId,
            executionId,
            PipelineContractDescriptor.DEFAULT_PIPELINE_ID,
            PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION,
            PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION,
            currentStepIndex,
            Optional.empty(),
            Optional.empty());
    }

    public PipelineExecutionContext {
        requireText(tenantId, "tenantId");
        requireText(executionId, "executionId");
        requireText(pipelineId, "pipelineId");
        requireText(contractVersion, "contractVersion");
        requireText(releaseVersion, "releaseVersion");
        if (currentStepIndex < 0) {
            throw new IllegalArgumentException("currentStepIndex must be non-negative");
        }
        correlationId = optionalText(correlationId, "correlationId");
        traceId = optionalText(traceId, "traceId");
    }

    public PipelineExecutionContext atStep(int stepIndex) {
        return new PipelineExecutionContext(
            tenantId, executionId, pipelineId, contractVersion, releaseVersion, stepIndex, correlationId, traceId);
    }

    private static void requireText(String value, String label) {
        if (value == null || value.isBlank() || !value.equals(value.trim())) {
            throw new IllegalArgumentException(label + " must be non-blank without surrounding whitespace");
        }
    }

    private static Optional<String> optionalText(Optional<String> value, String label) {
        Optional<String> actual = Objects.requireNonNull(value, label + " must not be null");
        actual.ifPresent(text -> requireText(text, label));
        return actual;
    }
}
