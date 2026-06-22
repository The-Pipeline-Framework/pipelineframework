package org.pipelineframework.orchestrator.release;

import java.util.Objects;

/**
 * Local/dev registry record for one activated pipeline release.
 */
public record PipelineReleaseRecord(
    String tenantId,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    PipelineReleaseStatus status,
    PipelineReleaseDescriptor descriptor,
    String primaryArtifactId,
    String primaryArtifactDigest,
    String primaryArtifactUri,
    long primaryArtifactSizeBytes,
    String primaryArtifactChecksum,
    PipelineContractDescriptor contract,
    long createdAtEpochMs,
    long updatedAtEpochMs,
    long activatedAtEpochMs
) {
    public PipelineReleaseRecord {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(contractVersion, "contractVersion");
        Objects.requireNonNull(releaseVersion, "releaseVersion");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(descriptor, "descriptor");
        primaryArtifactId = primaryArtifactId == null ? "" : primaryArtifactId;
        primaryArtifactDigest = primaryArtifactDigest == null ? "" : primaryArtifactDigest;
        primaryArtifactUri = primaryArtifactUri == null ? "" : primaryArtifactUri;
        primaryArtifactChecksum = primaryArtifactChecksum == null ? "" : primaryArtifactChecksum;
    }

    public PipelineReleaseRecord withStatus(PipelineReleaseStatus newStatus, long nowEpochMs) {
        return new PipelineReleaseRecord(
            tenantId,
            pipelineId,
            contractVersion,
            releaseVersion,
            newStatus,
            descriptor,
            primaryArtifactId,
            primaryArtifactDigest,
            primaryArtifactUri,
            primaryArtifactSizeBytes,
            primaryArtifactChecksum,
            contract,
            createdAtEpochMs,
            nowEpochMs,
            newStatus == PipelineReleaseStatus.ACTIVE ? nowEpochMs : activatedAtEpochMs);
    }
}
