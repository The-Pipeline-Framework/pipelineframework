package org.pipelineframework.processor.block;

import java.util.List;
import java.util.Map;

/** Build-time provenance for one packaged pipeline definition linked into an application. */
public record ImportedPipelineDefinition(
    String qualifiedId,
    String logicalName,
    String namespace,
    String groupId,
    String artifactId,
    String version,
    String resource,
    String definitionFingerprint,
    String linkedDefinitionFingerprint,
    List<ResolvedBlockRequirement> resolvedRequirements
) {
    public ImportedPipelineDefinition(
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

    public ImportedPipelineDefinition {
        requireText(qualifiedId, "qualifiedId");
        requireText(logicalName, "logicalName");
        requireText(namespace, "namespace");
        requireText(groupId, "groupId");
        requireText(artifactId, "artifactId");
        requireText(version, "version");
        requireText(resource, "resource");
        requireText(definitionFingerprint, "definitionFingerprint");
        requireText(linkedDefinitionFingerprint, "linkedDefinitionFingerprint");
        resolvedRequirements = resolvedRequirements == null ? List.of() : List.copyOf(resolvedRequirements);
    }

    /** Sanitized application resolution of one Block capability requirement. */
    public record ResolvedBlockRequirement(
        String name,
        String kind,
        String binding,
        String provider,
        int providerVersion,
        List<ResolvedOperation> operations,
        String commandIdGenerator,
        String duplicatePolicy,
        Map<String, Object> commandPolicy,
        String connectorConfigurationDigest
    ) {
        public ResolvedBlockRequirement {
            requireText(name, "requirement.name");
            requireText(kind, "requirement.kind");
            requireText(binding, "requirement.binding");
            requireText(provider, "requirement.provider");
            if (providerVersion < 1) {
                throw new IllegalArgumentException("requirement.providerVersion must be positive");
            }
            operations = operations == null ? List.of() : List.copyOf(operations);
            commandPolicy = commandPolicy == null ? Map.of() : Map.copyOf(commandPolicy);
            connectorConfigurationDigest = connectorConfigurationDigest == null
                ? "" : connectorConfigurationDigest;
        }
    }

    public record ResolvedOperation(String id, int version) {
        public ResolvedOperation {
            requireText(id, "operation.id");
            if (version < 1) {
                throw new IllegalArgumentException("operation.version must be positive");
            }
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
