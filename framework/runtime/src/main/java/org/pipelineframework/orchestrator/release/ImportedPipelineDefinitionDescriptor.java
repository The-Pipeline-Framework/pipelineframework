package org.pipelineframework.orchestrator.release;

/** Reproducible package provenance for a build-time linked pipeline definition. */
public record ImportedPipelineDefinitionDescriptor(
    String qualifiedId,
    String logicalName,
    String namespace,
    String groupId,
    String artifactId,
    String version,
    String resource,
    String definitionFingerprint
) {
}
