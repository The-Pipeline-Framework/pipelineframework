package org.pipelineframework.representation.spi;

/** Provider content description. Providers never receive a file-system or renderer handle. */
public record ArtifactDescription(
    String providerKey,
    ArtifactPhase phase,
    ArtifactKind kind,
    String logicalPath,
    String content,
    int providerOrdinal
) {
    public ArtifactDescription {
        if (providerKey == null || providerKey.isBlank() || phase == null || kind == null
            || logicalPath == null || logicalPath.isBlank() || content == null) {
            throw new IllegalArgumentException("artifact description fields must be present");
        }
        providerKey = providerKey.trim();
        logicalPath = logicalPath.trim();
    }
}
