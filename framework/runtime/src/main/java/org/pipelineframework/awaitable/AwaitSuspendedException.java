package org.pipelineframework.awaitable;

/**
 * Internal signal used to suspend queue-async execution at an await step.
 */
public class AwaitSuspendedException extends RuntimeException {
    private final String tenantId;
    private final String executionId;
    private final String interactionId;
    private final int stepIndex;

    public AwaitSuspendedException(String tenantId, String executionId, String interactionId, int stepIndex) {
        super("Pipeline execution is waiting for await interaction " + interactionId);
        this.tenantId = tenantId;
        this.executionId = executionId;
        this.interactionId = interactionId;
        this.stepIndex = stepIndex;
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
