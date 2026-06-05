package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Failure details carried by a transition result envelope.
 *
 * @param failureClass failure class name
 * @param message failure message
 */
public record TransitionFailureEnvelope(
    String failureClass,
    String message
) {
    public TransitionFailureEnvelope {
        Objects.requireNonNull(failureClass, "failureClass");
    }

    public static TransitionFailureEnvelope from(Throwable failure) {
        Objects.requireNonNull(failure, "failure");
        return new TransitionFailureEnvelope(failure.getClass().getName(), failure.getMessage());
    }

    public RuntimeException toException() {
        String suffix = message == null || message.isBlank() ? "" : ": " + message;
        return new TransitionWorkerFailureException(failureClass + suffix);
    }
}
