package org.pipelineframework.orchestrator;

/**
 * Exception used to route explicit FAILED worker results through retry/DLQ handling.
 */
public class TransitionWorkerFailureException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TransitionWorkerFailureException(String message) {
        super(message);
    }

    public TransitionWorkerFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}
