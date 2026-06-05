package org.pipelineframework.orchestrator;

/**
 * Internal SPI for local control-plane admission checks.
 */
public interface ControlPlaneAdmissionPolicy {

    /**
     * Admit or deny one control-plane operation.
     *
     * @param request admission request
     * @return admission decision
     */
    ControlPlaneAdmissionDecision admit(ControlPlaneAdmissionRequest request);

    /**
     * Admit or deny one transition and optionally acquire a tenant-scoped permit.
     *
     * @param request admission request
     * @return transition admission result
     */
    default ControlPlaneTransitionAdmission admitTransition(ControlPlaneAdmissionRequest request) {
        ControlPlaneAdmissionDecision decision = admit(request);
        if (!decision.allowed()) {
            return ControlPlaneTransitionAdmission.denied(decision);
        }
        return ControlPlaneTransitionAdmission.admitted(ControlPlaneTransitionPermit.noop());
    }
}
