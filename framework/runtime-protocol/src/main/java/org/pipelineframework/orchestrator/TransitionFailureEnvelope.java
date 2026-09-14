package org.pipelineframework.orchestrator;

import java.util.Objects;
import java.util.Optional;

/**
 * Failure details carried by a transition result envelope.
 *
 * @param failureClass failure class name
 * @param message failure message
 * @param failedStepIndex failed pipeline step index, or {@code -1} when unavailable
 * @param failedCommandId exact retryable logical Command effect
 */
public record TransitionFailureEnvelope(
    String failureClass,
    String message,
    int failedStepIndex,
    Optional<String> failedCommandId
) {
    public TransitionFailureEnvelope {
        Objects.requireNonNull(failureClass, "failureClass");
        failedCommandId = Optional.ofNullable(failedCommandId).orElseGet(Optional::empty);
    }

    public TransitionFailureEnvelope(String failureClass, String message) {
        this(failureClass, message, -1, Optional.empty());
    }

    public TransitionFailureEnvelope(String failureClass, String message, int failedStepIndex) {
        this(failureClass, message, failedStepIndex, Optional.empty());
    }

}
