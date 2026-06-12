package org.pipelineframework.orchestrator;

import java.util.Objects;

public record PipelineReleaseStoredArtifact(
    String artifactUri,
    long artifactSizeBytes,
    String artifactChecksum
) {
    public PipelineReleaseStoredArtifact {
        Objects.requireNonNull(artifactUri, "artifactUri");
        Objects.requireNonNull(artifactChecksum, "artifactChecksum");
        if (artifactSizeBytes < 0) {
            throw new IllegalArgumentException("artifactSizeBytes must be non-negative");
        }
    }
}
