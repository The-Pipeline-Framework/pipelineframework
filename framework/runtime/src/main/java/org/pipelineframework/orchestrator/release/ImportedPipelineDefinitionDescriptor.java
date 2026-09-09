package org.pipelineframework.orchestrator.release;

import java.util.List;
import java.util.Objects;

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
    List<ImportedBlockRequirementDescriptor> resolvedRequirements,
    List<ImportedBlockCallableDescriptor> resolvedCallables
) {
    public ImportedPipelineDefinitionDescriptor {
        linkedDefinitionFingerprint = linkedDefinitionFingerprint == null || linkedDefinitionFingerprint.isBlank()
            ? definitionFingerprint : linkedDefinitionFingerprint;
        resolvedRequirements = resolvedRequirements == null ? List.of() : List.copyOf(resolvedRequirements);
        resolvedCallables = List.copyOf(Objects.requireNonNull(
            resolvedCallables, "resolved Block callables must not be null"));
    }
}
