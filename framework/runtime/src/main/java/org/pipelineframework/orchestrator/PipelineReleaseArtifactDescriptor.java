package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Objects;

/**
 * One deployable artifact that satisfies a pipeline release.
 */
public record PipelineReleaseArtifactDescriptor(
    String artifactId,
    String kind,
    String uri,
    String digest,
    String bundleVersionId,
    String bundleHash,
    List<String> stepIds,
    List<String> capabilities
) {
    public PipelineReleaseArtifactDescriptor {
        Objects.requireNonNull(artifactId, "artifactId");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(uri, "uri");
        Objects.requireNonNull(digest, "digest");
        bundleVersionId = bundleVersionId == null ? "" : bundleVersionId;
        bundleHash = bundleHash == null ? "" : bundleHash;
        stepIds = stepIds == null ? List.of() : List.copyOf(stepIds);
        capabilities = capabilities == null ? List.of() : List.copyOf(capabilities);
    }
}
