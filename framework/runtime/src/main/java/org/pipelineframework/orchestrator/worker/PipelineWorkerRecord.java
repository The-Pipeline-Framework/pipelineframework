package org.pipelineframework.orchestrator.worker;

import java.util.Objects;

/**
 * Current worker lifecycle view derived from registration and heartbeat events.
 */
public record PipelineWorkerRecord(
    String tenantId,
    String pipelineId,
    String contractVersion,
    String releaseVersion,
    String workerId,
    String protocol,
    String endpoint,
    String artifactId,
    String artifactDigest,
    PipelineWorkerState state,
    long registeredAtEpochMs,
    long lastHeartbeatAtEpochMs,
    long drainingSinceEpochMs
) {
    public PipelineWorkerRecord {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(contractVersion, "contractVersion");
        Objects.requireNonNull(releaseVersion, "releaseVersion");
        Objects.requireNonNull(workerId, "workerId");
        Objects.requireNonNull(protocol, "protocol");
        Objects.requireNonNull(state, "state");
        endpoint = endpoint == null ? "" : endpoint;
        artifactId = artifactId == null ? "" : artifactId;
        artifactDigest = artifactDigest == null ? "" : artifactDigest;
    }

    public boolean matches(PipelineWorkerAvailabilityRequest request, String providerName) {
        return sameReleaseAndProvider(request, providerName)
            && (request.artifactId().isBlank() || artifactId.isBlank() || request.artifactId().equals(artifactId))
            && (request.artifactDigest().isBlank()
                || artifactDigest.isBlank()
                || request.artifactDigest().equals(artifactDigest));
    }

    public boolean hasArtifactMismatch(PipelineWorkerAvailabilityRequest request, String providerName) {
        return sameReleaseAndProvider(request, providerName)
            && ((!request.artifactId().isBlank() && !artifactId.isBlank() && !request.artifactId().equals(artifactId))
            || (!request.artifactDigest().isBlank()
                && !artifactDigest.isBlank()
                && !request.artifactDigest().equals(artifactDigest)));
    }

    public boolean sameReleaseAndProvider(PipelineWorkerAvailabilityRequest request, String providerName) {
        return protocol.equalsIgnoreCase(providerName)
            && tenantId.equals(request.tenantId())
            && pipelineId.equals(request.pipelineId())
            && contractVersion.equals(request.contractVersion())
            && releaseVersion.equals(request.releaseVersion());
    }
}
