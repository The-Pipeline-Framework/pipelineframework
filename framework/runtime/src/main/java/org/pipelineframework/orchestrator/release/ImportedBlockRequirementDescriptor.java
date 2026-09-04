package org.pipelineframework.orchestrator.release;

import java.util.List;
import java.util.Map;

/** Sanitized application resolution of one imported Block capability. */
public record ImportedBlockRequirementDescriptor(
    String name,
    String kind,
    String binding,
    String provider,
    int providerVersion,
    List<ImportedBlockOperationDescriptor> operations,
    String commandIdGenerator,
    String duplicatePolicy,
    Map<String, Object> commandPolicy,
    String connectorConfigurationDigest
) {
    public ImportedBlockRequirementDescriptor {
        operations = operations == null ? List.of() : List.copyOf(operations);
        commandPolicy = commandPolicy == null ? Map.of() : Map.copyOf(commandPolicy);
        connectorConfigurationDigest = connectorConfigurationDigest == null ? "" : connectorConfigurationDigest;
    }
}
