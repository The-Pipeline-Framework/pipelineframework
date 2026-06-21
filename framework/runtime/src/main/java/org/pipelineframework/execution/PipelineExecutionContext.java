package org.pipelineframework.execution;

/**
 * Framework-managed execution scope for steps that need durable pipeline identity.
 */
public record PipelineExecutionContext(String tenantId, String executionId, int currentStepIndex) {
    public PipelineExecutionContext {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
        if (currentStepIndex < 0) {
            throw new IllegalArgumentException("currentStepIndex must be non-negative");
        }
    }
}
