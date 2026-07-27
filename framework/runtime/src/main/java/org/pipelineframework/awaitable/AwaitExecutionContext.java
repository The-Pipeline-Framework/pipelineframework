package org.pipelineframework.awaitable;

/**
 * Internal queue-async execution context used by generated await steps.
 */
public final class AwaitExecutionContext {
    private final String tenantId;
    private final String executionId;
    private final boolean durableAwaitBoundary;
    private int currentStepIndex;

    public AwaitExecutionContext(String tenantId, String executionId, int currentStepIndex) {
        this(tenantId, executionId, currentStepIndex, false);
    }

    /**
     * Creates a queue-async context.
     *
     * @param tenantId tenant identifier
     * @param executionId execution identifier
     * @param currentStepIndex current pipeline step index
     * @param durableAwaitBoundary whether awaits must hand off to durable continuation handling
     */
    public AwaitExecutionContext(
        String tenantId,
        String executionId,
        int currentStepIndex,
        boolean durableAwaitBoundary
    ) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
        if (currentStepIndex < 0) {
            throw new IllegalArgumentException("currentStepIndex must be non-negative");
        }
        this.tenantId = tenantId;
        this.executionId = executionId;
        this.currentStepIndex = currentStepIndex;
        this.durableAwaitBoundary = durableAwaitBoundary;
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

    /**
     * Whether this execution must suspend at an await boundary instead of retaining a live worker session.
     *
     * @return true for durable await handoff
     */
    public boolean durableAwaitBoundary() {
        return durableAwaitBoundary;
    }

    public void currentStepIndex(int currentStepIndex) {
        if (currentStepIndex < 0) {
            throw new IllegalArgumentException("currentStepIndex must be non-negative");
        }
        this.currentStepIndex = currentStepIndex;
    }
}
