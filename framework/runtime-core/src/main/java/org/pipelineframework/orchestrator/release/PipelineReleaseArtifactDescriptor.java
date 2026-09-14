package org.pipelineframework.orchestrator.release;

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
    List<String> stepIds,
    List<String> capabilities
) {
    public PipelineReleaseArtifactDescriptor {
        Objects.requireNonNull(artifactId, "artifactId");
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(uri, "uri");
        Objects.requireNonNull(digest, "digest");
        stepIds = stepIds == null ? List.of() : List.copyOf(stepIds);
        capabilities = capabilities == null ? List.of() : List.copyOf(capabilities);
    }
}
