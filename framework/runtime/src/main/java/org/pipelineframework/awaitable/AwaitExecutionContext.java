package org.pipelineframework.awaitable;

import java.util.Objects;
import org.pipelineframework.orchestrator.PipelineExecutionPosition;

/**
 * Internal queue-async execution context used by generated await steps.
 */
public final class AwaitExecutionContext {
    private final String tenantId;
    private final String executionId;
    private final AwaitContinuationMode continuationMode;
    private final TerminalOutputOwnership terminalOutputOwnership;
    private PipelineExecutionPosition currentPosition;

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
        this(
            tenantId,
            executionId,
            PipelineExecutionPosition.root(currentStepIndex),
            continuationMode,
            terminalOutputOwnership);
    }

    /**
     * Creates a queue-async context at a compiler-derived execution position.
     *
     * <p>The root cursor remains available through {@link #currentStepIndex()} for the existing
     * runner range API. The static location disambiguates an await inside a nested invocation.
     */
    public AwaitExecutionContext(
        String tenantId,
        String executionId,
        PipelineExecutionPosition currentPosition,
        AwaitContinuationMode continuationMode,
        TerminalOutputOwnership terminalOutputOwnership
    ) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
        this.tenantId = tenantId;
        this.executionId = executionId;
        this.currentPosition = Objects.requireNonNull(currentPosition, "currentPosition must not be null");
        this.continuationMode = Objects.requireNonNull(continuationMode, "continuationMode must not be null");
        this.terminalOutputOwnership = Objects.requireNonNull(
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
        return currentPosition.rootStepIndex();
    }

    public PipelineExecutionPosition currentPosition() {
        return currentPosition;
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
        this.currentPosition = PipelineExecutionPosition.root(currentStepIndex);
    }

    public AwaitExecutionContext atRootStep(int rootStepIndex) {
        PipelineExecutionPosition position = currentPosition.nested()
            && currentPosition.rootStepIndex() == rootStepIndex
            ? currentPosition
            : PipelineExecutionPosition.root(rootStepIndex);
        return new AwaitExecutionContext(
            tenantId, executionId, position, continuationMode, terminalOutputOwnership);
    }

    public AwaitExecutionContext atPosition(PipelineExecutionPosition position) {
        return new AwaitExecutionContext(
            tenantId, executionId, position, continuationMode, terminalOutputOwnership);
    }
}
