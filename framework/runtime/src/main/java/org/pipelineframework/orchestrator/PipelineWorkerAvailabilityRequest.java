package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Bundle identity a hosted coordinator needs from the selected worker.
 *
 * @param tenantId tenant id
 * @param pipelineId pinned pipeline id
 * @param bundleVersionId pinned bundle version id
 */
public record PipelineWorkerAvailabilityRequest(
    String tenantId,
    String pipelineId,
    String bundleVersionId
) {
    public PipelineWorkerAvailabilityRequest {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(bundleVersionId, "bundleVersionId");
    }
}
