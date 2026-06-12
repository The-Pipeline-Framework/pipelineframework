package org.pipelineframework.orchestrator;

import java.util.Objects;

public record PipelineReleaseStoredArtifact(
    String artifactPath,
    long artifactSizeBytes,
    String artifactChecksum
) {
    public PipelineReleaseStoredArtifact {
        Objects.requireNonNull(artifactPath, "artifactPath");
        Objects.requireNonNull(artifactChecksum, "artifactChecksum");
        if (artifactSizeBytes < 0) {
            throw new IllegalArgumentException("artifactSizeBytes must be non-negative");
        }
    }
}
