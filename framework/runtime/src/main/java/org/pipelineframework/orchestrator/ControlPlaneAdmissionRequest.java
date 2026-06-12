package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Admission request for one local control-plane operation.
 *
 * @param tenantId normalized tenant id
 * @param operation operation being attempted
 * @param pipelineId effective pipeline id
 * @param releaseVersion effective release version
 * @param executionId execution id when known
 * @param source source/protocol metadata, for example api or worker-dispatch
 * @param explicitTenant whether the caller supplied a tenant before defaulting
 */
public record ControlPlaneAdmissionRequest(
    String tenantId,
    ControlPlaneAdmissionOperation operation,
    String pipelineId,
    String releaseVersion,
    String executionId,
    String source,
    boolean explicitTenant
) {
    public ControlPlaneAdmissionRequest {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(operation, "operation");
        source = source == null || source.isBlank() ? "unknown" : source.trim();
    }
}
