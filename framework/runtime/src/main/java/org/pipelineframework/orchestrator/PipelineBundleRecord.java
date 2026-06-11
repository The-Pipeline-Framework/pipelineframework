package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Local/dev registry record for one versioned pipeline bundle artifact.
 */
public record PipelineBundleRecord(
    String tenantId,
    String pipelineId,
    String bundleVersionId,
    String bundleHash,
    String artifactPath,
    long artifactSizeBytes,
    String artifactChecksum,
    PipelineBundleStatus status,
    PipelineBundleManifest manifest,
    long createdAtEpochMs,
    long updatedAtEpochMs,
    long activatedAtEpochMs
) {
    public PipelineBundleRecord {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(bundleVersionId, "bundleVersionId");
        Objects.requireNonNull(bundleHash, "bundleHash");
        Objects.requireNonNull(artifactPath, "artifactPath");
        Objects.requireNonNull(artifactChecksum, "artifactChecksum");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(manifest, "manifest");
    }

    public PipelineBundleRecord(
        String tenantId,
        String pipelineId,
        String bundleVersionId,
        String bundleHash,
        String artifactPath,
        PipelineBundleStatus status,
        PipelineBundleManifest manifest,
        long createdAtEpochMs,
        long updatedAtEpochMs,
        long activatedAtEpochMs
    ) {
        this(
            tenantId,
            pipelineId,
            bundleVersionId,
            bundleHash,
            artifactPath,
            -1L,
            "",
            status,
            manifest,
            createdAtEpochMs,
            updatedAtEpochMs,
            activatedAtEpochMs);
    }

    public PipelineBundleRecord withStatus(PipelineBundleStatus newStatus, long nowEpochMs) {
        return new PipelineBundleRecord(
            tenantId,
            pipelineId,
            bundleVersionId,
            bundleHash,
            artifactPath,
            artifactSizeBytes,
            artifactChecksum,
            newStatus,
            manifest,
            createdAtEpochMs,
            nowEpochMs,
            newStatus == PipelineBundleStatus.ACTIVE ? nowEpochMs : activatedAtEpochMs);
    }
}
