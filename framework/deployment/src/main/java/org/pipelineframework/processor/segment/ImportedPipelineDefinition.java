package org.pipelineframework.processor.segment;

/** Build-time provenance for one packaged pipeline definition linked into an application. */
public record ImportedPipelineDefinition(
    String qualifiedId,
    String logicalName,
    String namespace,
    String groupId,
    String artifactId,
    String version,
    String resource,
    String definitionFingerprint
) {
    public ImportedPipelineDefinition {
        requireText(qualifiedId, "qualifiedId");
        requireText(logicalName, "logicalName");
        requireText(namespace, "namespace");
        requireText(groupId, "groupId");
        requireText(artifactId, "artifactId");
        requireText(version, "version");
        requireText(resource, "resource");
        requireText(definitionFingerprint, "definitionFingerprint");
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
