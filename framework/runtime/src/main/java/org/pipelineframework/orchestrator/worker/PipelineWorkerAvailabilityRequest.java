package org.pipelineframework.orchestrator.worker;

import org.pipelineframework.orchestrator.release.PipelineContractDescriptor;
import java.util.Objects;

/**
 * Release identity a hosted coordinator needs from the selected worker.
 *
 * @param tenantId tenant id
 * @param pipelineId pinned pipeline id
 * @param contractVersion pinned contract version
 * @param releaseVersion pinned release version
 * @param artifactId pinned artifact id, when known
 * @param artifactDigest pinned artifact digest, when known
 */
public record PipelineWorkerAvailabilityRequest(
    String tenantId,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    String artifactId,
    String artifactDigest
) {
    public PipelineWorkerAvailabilityRequest(
        String tenantId,
        String pipelineId,
        String releaseVersion
    ) {
        this(
            tenantId,
            pipelineId,
            PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION,
            releaseVersion,
            "",
            "");
    }

    public PipelineWorkerAvailabilityRequest {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        contractVersion = contractVersion == null || contractVersion.isBlank()
            ? PipelineContractDescriptor.DEFAULT_CONTRACT_VERSION
            : contractVersion;
        releaseVersion = releaseVersion == null || releaseVersion.isBlank()
            ? contractVersion
            : releaseVersion;
        artifactId = artifactId == null ? "" : artifactId;
        artifactDigest = artifactDigest == null ? "" : artifactDigest;
    }
}
