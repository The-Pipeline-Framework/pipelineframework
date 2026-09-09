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
    List<ResolvedBlockRequirement> resolvedRequirements,
    List<ResolvedBlockCallable> resolvedCallables
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
            definitionFingerprint, definitionFingerprint, List.of(), List.of());
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
        resolvedCallables = List.copyOf(java.util.Objects.requireNonNull(
            resolvedCallables, "resolvedCallables must not be null"));
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

    /** Sanitized linked identity of one callable exposed by an imported Block decision step. */
    public record ResolvedBlockCallable(
        String sourceStep,
        String alias,
        String requirement,
        String kind,
        String binding,
        String provider,
        int providerVersion,
        String operation,
        int operationVersion,
        String input,
        String output,
        Map<String, String> trustedArguments,
        String commandIdGenerator,
        String duplicatePolicy,
        Map<String, Object> commandPolicy,
        String connectorConfigurationDigest
    ) {
        public ResolvedBlockCallable {
            requireText(sourceStep, "callable.sourceStep");
            requireText(alias, "callable.alias");
            requireText(requirement, "callable.requirement");
            requireText(kind, "callable.kind");
            requireText(binding, "callable.binding");
            requireText(provider, "callable.provider");
            if (providerVersion < 1 || operationVersion < 1) {
                throw new IllegalArgumentException("callable provider and operation versions must be positive");
            }
            requireText(operation, "callable.operation");
            requireText(input, "callable.input");
            requireText(output, "callable.output");
            trustedArguments = Map.copyOf(java.util.Objects.requireNonNull(
                trustedArguments, "callable.trustedArguments must not be null"));
            commandIdGenerator = java.util.Objects.requireNonNull(
                commandIdGenerator, "callable.commandIdGenerator must not be null");
            duplicatePolicy = java.util.Objects.requireNonNull(
                duplicatePolicy, "callable.duplicatePolicy must not be null");
            commandPolicy = Map.copyOf(java.util.Objects.requireNonNull(
                commandPolicy, "callable.commandPolicy must not be null"));
            connectorConfigurationDigest = java.util.Objects.requireNonNull(
                connectorConfigurationDigest, "callable.connectorConfigurationDigest must not be null");
        }
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
