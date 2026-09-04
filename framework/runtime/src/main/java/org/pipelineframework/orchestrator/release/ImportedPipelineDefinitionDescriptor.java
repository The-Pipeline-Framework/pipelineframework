package org.pipelineframework.orchestrator.release;

import java.util.List;

/** Reproducible package provenance for a build-time linked pipeline definition. */
public record ImportedPipelineDefinitionDescriptor(
    String qualifiedId,
    String logicalName,
    String namespace,
    String groupId,
    String artifactId,
    String version,
    String resource,
    String definitionFingerprint,
    String linkedDefinitionFingerprint,
    List<ImportedBlockRequirementDescriptor> resolvedRequirements
) {
    public ImportedPipelineDefinitionDescriptor {
        linkedDefinitionFingerprint = linkedDefinitionFingerprint == null || linkedDefinitionFingerprint.isBlank()
            ? definitionFingerprint : linkedDefinitionFingerprint;
        resolvedRequirements = resolvedRequirements == null ? List.of() : List.copyOf(resolvedRequirements);
    }

    /** Source compatibility for schema-v3 contracts emitted before application-bound Block capabilities. */
    public ImportedPipelineDefinitionDescriptor(
        String qualifiedId,
        String logicalName,
        String namespace,
        String groupId,
        String artifactId,
        String version,
        String resource,
        String definitionFingerprint
    ) {
        this(qualifiedId, logicalName, namespace, groupId, artifactId, version, resource,
            definitionFingerprint, definitionFingerprint, List.of());
    }
}
