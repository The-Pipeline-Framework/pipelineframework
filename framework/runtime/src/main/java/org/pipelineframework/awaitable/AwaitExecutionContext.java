package org.pipelineframework.awaitable;

/**
 * Internal queue-async execution context used by generated await steps.
 */
public final class AwaitExecutionContext {
    private final String tenantId;
    private final String executionId;
    private final AwaitContinuationMode continuationMode;
    private final TerminalOutputOwnership terminalOutputOwnership;
    private int currentStepIndex;

    public AwaitExecutionContext(String tenantId, String executionId, int currentStepIndex) {
        this(
            tenantId,
            executionId,
            currentStepIndex,
            AwaitContinuationMode.LIVE_IF_SUPPORTED,
            TerminalOutputOwnership.TRANSITION_WORKER);
    }

    /**
     * Creates a queue-async context.
     *
     * @param tenantId tenant identifier
     * @param executionId execution identifier
     * @param currentStepIndex current pipeline step index
     * @param continuationMode whether an await may use a live completion window
     * @param terminalOutputOwnership side of the transition boundary that publishes terminal output
     */
    public AwaitExecutionContext(
        String tenantId,
        String executionId,
        int currentStepIndex,
        AwaitContinuationMode continuationMode,
        TerminalOutputOwnership terminalOutputOwnership
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
        this.continuationMode = java.util.Objects.requireNonNull(continuationMode, "continuationMode must not be null");
        this.terminalOutputOwnership = java.util.Objects.requireNonNull(
            terminalOutputOwnership,
            "terminalOutputOwnership must not be null");
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
     * Returns the await continuation mode for this transition.
     *
     * @return continuation mode
     */
    public AwaitContinuationMode continuationMode() {
        return continuationMode;
    }

    /**
     * Returns which side of the boundary owns terminal object publication.
     *
     * @return terminal output owner
     */
    public TerminalOutputOwnership terminalOutputOwnership() {
        return terminalOutputOwnership;
    }

    public void currentStepIndex(int currentStepIndex) {
        if (currentStepIndex < 0) {
            throw new IllegalArgumentException("currentStepIndex must be non-negative");
        }
        this.currentStepIndex = currentStepIndex;
    }
}
