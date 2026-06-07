package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Bundle identity a hosted coordinator needs from the selected worker.
 *
 * @param tenantId tenant id
 * @param pipelineId pinned pipeline id
 * @param contractVersion pinned contract version
 * @param releaseVersion pinned release version
 * @param bundleVersionId pinned bundle version id
 */
public record PipelineWorkerAvailabilityRequest(
    String tenantId,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    String bundleVersionId
) {
    public PipelineWorkerAvailabilityRequest(
        String tenantId,
        String pipelineId,
        String bundleVersionId
    ) {
        this(
            tenantId,
            pipelineId,
            PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION,
            bundleVersionId,
            bundleVersionId);
    }

    public PipelineWorkerAvailabilityRequest {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        contractVersion = contractVersion == null || contractVersion.isBlank()
            ? PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION
            : contractVersion;
        releaseVersion = releaseVersion == null || releaseVersion.isBlank()
            ? bundleVersionId
            : releaseVersion;
        Objects.requireNonNull(bundleVersionId, "bundleVersionId");
    }
}
