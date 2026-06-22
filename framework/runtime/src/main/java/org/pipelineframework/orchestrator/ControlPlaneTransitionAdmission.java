package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Admission result for a tenant-scoped transition execution.
 *
 * @param decision admission decision
 * @param permit permit to release when the transition attempt finishes
 */
public record ControlPlaneTransitionAdmission(
    ControlPlaneAdmissionDecision decision,
    ControlPlaneTransitionPermit permit
) {
    /**
     * Creates an admitted transition result.
     *
     * @param permit acquired permit
     * @return admitted transition result
     */
    public static ControlPlaneTransitionAdmission admitted(ControlPlaneTransitionPermit permit) {
        return new ControlPlaneTransitionAdmission(ControlPlaneAdmissionDecision.allow(), permit);
    }

    /**
     * Creates a denied transition result.
     *
     * @param decision denial decision
     * @return denied transition result
     */
    public static ControlPlaneTransitionAdmission denied(ControlPlaneAdmissionDecision decision) {
        ControlPlaneAdmissionDecision required = Objects.requireNonNull(decision, "decision");
        if (required.allowed()) {
            throw new IllegalArgumentException("Denied transition admission requires a denied decision");
        }
        return new ControlPlaneTransitionAdmission(required, null);
    }
}
