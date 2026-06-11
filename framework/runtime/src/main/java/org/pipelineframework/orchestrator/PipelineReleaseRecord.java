package org.pipelineframework.orchestrator;

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
    String bundleVersionId,
    String bundleHash,
    String primaryArtifactPath,
    long primaryArtifactSizeBytes,
    String primaryArtifactChecksum,
    PipelineBundleManifest manifest,
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
        bundleVersionId = bundleVersionId == null ? "" : bundleVersionId;
        bundleHash = bundleHash == null ? "" : bundleHash;
        primaryArtifactPath = primaryArtifactPath == null ? "" : primaryArtifactPath;
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
            bundleVersionId,
            bundleHash,
            primaryArtifactPath,
            primaryArtifactSizeBytes,
            primaryArtifactChecksum,
            manifest,
            createdAtEpochMs,
            nowEpochMs,
            newStatus == PipelineReleaseStatus.ACTIVE ? nowEpochMs : activatedAtEpochMs);
    }
}
