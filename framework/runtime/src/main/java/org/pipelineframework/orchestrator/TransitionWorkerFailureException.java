package org.pipelineframework.orchestrator;

/**
 * Exception used to route explicit FAILED worker results through retry/DLQ handling.
 */
public class TransitionWorkerFailureException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int failedStepIndex;
    private final String failedCommandId;

    public TransitionWorkerFailureException(String message) {
        super(message);
        this.failedStepIndex = -1;
        this.failedCommandId = null;
    }

    public TransitionWorkerFailureException(String message, Throwable cause) {
        this(message, cause, -1);
    }

    public TransitionWorkerFailureException(String message, int failedStepIndex) {
        this(message, failedStepIndex, null);
    }

    public TransitionWorkerFailureException(String message, int failedStepIndex, String failedCommandId) {
        super(message);
        this.failedStepIndex = failedStepIndex;
        this.failedCommandId = failedCommandId;
    }

    public TransitionWorkerFailureException(String message, Throwable cause, int failedStepIndex) {
        super(message, cause);
        this.failedStepIndex = failedStepIndex;
        this.failedCommandId = null;
    }

    public int failedStepIndex() {
        return failedStepIndex;
    }

    public String failedCommandId() {
        return failedCommandId;
    }
}
