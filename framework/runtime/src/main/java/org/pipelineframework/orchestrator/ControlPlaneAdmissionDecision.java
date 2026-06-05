package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Decision returned by a local control-plane admission policy.
 *
 * @param allowed whether the operation is admitted
 * @param errorCode stable denial code
 * @param reason human-readable denial reason
 */
public record ControlPlaneAdmissionDecision(
    boolean allowed,
    String errorCode,
    String reason
) {
    public static final String TENANT_REQUIRED = "TENANT_REQUIRED";
    public static final String TENANT_NOT_ALLOWED = "TENANT_NOT_ALLOWED";
    public static final String TENANT_TRANSITION_QUOTA_SATURATED = "TENANT_TRANSITION_QUOTA_SATURATED";

    public ControlPlaneAdmissionDecision {
        if (!allowed) {
            Objects.requireNonNull(errorCode, "errorCode is required when admission is denied");
            Objects.requireNonNull(reason, "reason is required when admission is denied");
        } else if (errorCode != null || reason != null) {
            throw new IllegalArgumentException("errorCode and reason must be null when admission is allowed");
        }
    }

    /**
     * Creates an allow decision.
     *
     * @return allow decision
     */
    public static ControlPlaneAdmissionDecision allow() {
        return new ControlPlaneAdmissionDecision(true, null, null);
    }

    /**
     * Creates a deny decision.
     *
     * @param errorCode stable denial code
     * @param reason denial reason
     * @return deny decision
     */
    public static ControlPlaneAdmissionDecision deny(String errorCode, String reason) {
        return new ControlPlaneAdmissionDecision(false, errorCode, reason);
    }
}
