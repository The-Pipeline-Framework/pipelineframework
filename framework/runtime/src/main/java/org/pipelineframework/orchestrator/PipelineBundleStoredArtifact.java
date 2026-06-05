package org.pipelineframework.orchestrator;

import java.util.Objects;

/**
 * Managed bundle artifact metadata after the coordinator copies a registered source JAR.
 *
 * @param artifactPath managed local artifact path
 * @param artifactSizeBytes artifact size in bytes
 * @param artifactChecksum SHA-256 checksum of the stored artifact bytes
 */
public record PipelineBundleStoredArtifact(
    String artifactPath,
    long artifactSizeBytes,
    String artifactChecksum
) {
    public PipelineBundleStoredArtifact {
        Objects.requireNonNull(artifactPath, "artifactPath");
        Objects.requireNonNull(artifactChecksum, "artifactChecksum");
        if (artifactSizeBytes < 0) {
            throw new IllegalArgumentException("artifactSizeBytes must be non-negative");
        }
    }
}
