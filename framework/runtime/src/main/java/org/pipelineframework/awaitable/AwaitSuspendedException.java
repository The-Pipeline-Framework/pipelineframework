package org.pipelineframework.awaitable;

import org.pipelineframework.step.NonRetryableException;

/**
 * Internal signal used to suspend queue-async execution at an await step.
 */
public class AwaitSuspendedException extends NonRetryableException {
    private final String tenantId;
    private final String executionId;
    private final String interactionId;
    private final int stepIndex;

    public AwaitSuspendedException(String tenantId, String executionId, String interactionId, int stepIndex) {
        super(message(tenantId, executionId, interactionId, stepIndex));
        this.tenantId = tenantId;
        this.executionId = executionId;
        this.interactionId = interactionId;
        this.stepIndex = stepIndex;
    }

    private static String message(String tenantId, String executionId, String interactionId, int stepIndex) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
        if (interactionId == null || interactionId.isBlank()) {
            throw new IllegalArgumentException("interactionId must not be blank");
        }
        if (stepIndex < 0) {
            throw new IllegalArgumentException("stepIndex must be >= 0");
        }
        return "Pipeline execution is waiting for await interaction " + interactionId;
    }

    public String tenantId() {
        return tenantId;
    }

    public String executionId() {
        return executionId;
    }

    public String interactionId() {
        return interactionId;
    }

    public int stepIndex() {
        return stepIndex;
    }
}
