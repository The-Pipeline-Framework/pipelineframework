package org.pipelineframework.awaitable;

import org.pipelineframework.step.PipelineControlFlowException;
import org.pipelineframework.orchestrator.PipelineExecutionPosition;

/**
 * Internal signal used to suspend queue-async execution at an await step.
 */
public class AwaitSuspendedException extends PipelineControlFlowException {
    private final String tenantId;
    private final String executionId;
    private final String unitId;
    private final int stepIndex;
    private final PipelineExecutionPosition position;

    public AwaitSuspendedException(String tenantId, String executionId, String unitId, int stepIndex) {
        this(tenantId, executionId, unitId, PipelineExecutionPosition.root(stepIndex));
    }

    public AwaitSuspendedException(
        String tenantId, String executionId, String unitId, PipelineExecutionPosition position
    ) {
        super(message(tenantId, executionId, unitId, position.rootStepIndex()));
        this.tenantId = tenantId;
        this.executionId = executionId;
        this.unitId = unitId;
        this.stepIndex = position.rootStepIndex();
        this.position = position;
    }

    private static String message(String tenantId, String executionId, String unitId, int stepIndex) {
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("tenantId must not be blank");
        }
        if (executionId == null || executionId.isBlank()) {
            throw new IllegalArgumentException("executionId must not be blank");
        }
        if (unitId == null || unitId.isBlank()) {
            throw new IllegalArgumentException("unitId must not be blank");
        }
        if (stepIndex < 0) {
            throw new IllegalArgumentException("stepIndex must be >= 0");
        }
        return "Pipeline execution is waiting for await unit " + unitId;
    }

    public String tenantId() {
        return tenantId;
    }

    public String executionId() {
        return executionId;
    }

    public String unitId() {
        return unitId;
    }

    public int stepIndex() {
        return stepIndex;
    }

    public PipelineExecutionPosition position() {
        return position;
    }
}
