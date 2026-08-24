package org.pipelineframework.orchestrator;

import java.util.Optional;

/**
 * Exception used to route explicit FAILED worker results through retry/DLQ handling.
 */
public class TransitionWorkerFailureException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int failedStepIndex;
    private final Optional<String> failedCommandId;

    public TransitionWorkerFailureException(String message) {
        super(message);
        this.failedStepIndex = -1;
        this.failedCommandId = Optional.empty();
    }

    public TransitionWorkerFailureException(String message, Throwable cause) {
        this(message, cause, -1);
    }

    public TransitionWorkerFailureException(String message, int failedStepIndex) {
        this(message, failedStepIndex, Optional.empty());
    }

    public TransitionWorkerFailureException(
        String message,
        int failedStepIndex,
        Optional<String> failedCommandId
    ) {
        super(message);
        this.failedStepIndex = failedStepIndex;
        this.failedCommandId = Optional.ofNullable(failedCommandId).orElseGet(Optional::empty);
    }

    public TransitionWorkerFailureException(String message, Throwable cause, int failedStepIndex) {
        super(message, cause);
        this.failedStepIndex = failedStepIndex;
        this.failedCommandId = Optional.empty();
    }

    public int failedStepIndex() {
        return failedStepIndex;
    }

    public Optional<String> failedCommandId() {
        return failedCommandId;
    }
}
