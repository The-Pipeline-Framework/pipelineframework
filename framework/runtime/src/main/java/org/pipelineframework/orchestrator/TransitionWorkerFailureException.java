package org.pipelineframework.orchestrator;

/**
 * Exception used to route explicit FAILED worker results through retry/DLQ handling.
 */
public class TransitionWorkerFailureException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int failedStepIndex;

    public TransitionWorkerFailureException(String message) {
        this(message, null, -1);
    }

    public TransitionWorkerFailureException(String message, Throwable cause) {
        this(message, cause, -1);
    }

    public TransitionWorkerFailureException(String message, int failedStepIndex) {
        this(message, null, failedStepIndex);
    }

    public TransitionWorkerFailureException(String message, Throwable cause, int failedStepIndex) {
        super(message, cause);
        this.failedStepIndex = failedStepIndex;
    }

    public int failedStepIndex() {
        return failedStepIndex;
    }
}
