package org.pipelineframework.awaitable;

/**
 * Internal queue-async execution context used by generated await steps.
 */
public final class AwaitExecutionContext {
    private final String tenantId;
    private final String executionId;
    private int currentStepIndex;

    public AwaitExecutionContext(String tenantId, String executionId, int currentStepIndex) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
        this.tenantId = tenantId;
        this.executionId = executionId;
        this.currentStepIndex = currentStepIndex;
    }

    public String tenantId() {
        return tenantId;
    }

    public String executionId() {
        return executionId;
    }

    public int currentStepIndex() {
        return currentStepIndex;
    }

    public void currentStepIndex(int currentStepIndex) {
        this.currentStepIndex = currentStepIndex;
    }
}
