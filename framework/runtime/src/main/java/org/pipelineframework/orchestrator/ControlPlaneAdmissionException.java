package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Runtime exception raised when a local control-plane operation is denied.
 */
public class ControlPlaneAdmissionException extends RuntimeException {

    private final ControlPlaneAdmissionDecision decision;

    public ControlPlaneAdmissionException(ControlPlaneAdmissionDecision decision) {
        super(message(decision));
        this.decision = decision;
    }

    private static String message(ControlPlaneAdmissionDecision decision) {
        ControlPlaneAdmissionDecision required = Objects.requireNonNull(decision, "decision");
        if (required.allowed()) {
            throw new IllegalArgumentException("ControlPlaneAdmissionException requires a denied decision");
        }
        return required.errorCode() + ": " + required.reason();
    }

    /**
     * Denial decision.
     *
     * @return admission decision
     */
    public ControlPlaneAdmissionDecision decision() {
        return decision;
    }
}
