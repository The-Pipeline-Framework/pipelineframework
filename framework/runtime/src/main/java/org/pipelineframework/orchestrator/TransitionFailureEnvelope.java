package org.pipelineframework.orchestrator;

import java.util.Objects;
import org.pipelineframework.step.NonRetryableException;

/**
 * Failure details carried by a transition result envelope.
 *
 * @param failureClass failure class name
 * @param message failure message
 * @param failedStepIndex failed pipeline step index, or {@code -1} when unavailable
 */
public record TransitionFailureEnvelope(
    String failureClass,
    String message,
    int failedStepIndex
) {
    public TransitionFailureEnvelope {
        Objects.requireNonNull(failureClass, "failureClass");
    }

    public TransitionFailureEnvelope(String failureClass, String message) {
        this(failureClass, message, -1);
    }

    public static TransitionFailureEnvelope from(Throwable failure) {
        Objects.requireNonNull(failure, "failure");
        return new TransitionFailureEnvelope(
            failure.getClass().getName(),
            java.util.Optional.ofNullable(failure.getMessage()).orElse(""),
            -1);
    }

    public static TransitionFailureEnvelope from(Throwable failure, int failedStepIndex) {
        Objects.requireNonNull(failure, "failure");
        return new TransitionFailureEnvelope(
            failure.getClass().getName(),
            java.util.Optional.ofNullable(failure.getMessage()).orElse(""),
            failedStepIndex);
    }

    public RuntimeException toException() {
        if (isNonRetryableFailureClass()) {
            return new NonRetryableException(message == null || message.isBlank() ? failureClass : message);
        }
        String suffix = message == null || message.isBlank() ? "" : ": " + message;
        return new TransitionWorkerFailureException(failureClass + suffix, failedStepIndex);
    }

    private boolean isNonRetryableFailureClass() {
        if (NonRetryableException.class.getName().equals(failureClass)) {
            return true;
        }
        try {
            ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
            ClassLoader loader = contextLoader == null
                ? TransitionFailureEnvelope.class.getClassLoader()
                : contextLoader;
            Class<?> failureType = Class.forName(failureClass, false, loader);
            return NonRetryableException.class.isAssignableFrom(failureType);
        } catch (ClassNotFoundException | LinkageError ignored) {
            return false;
        }
    }
}
