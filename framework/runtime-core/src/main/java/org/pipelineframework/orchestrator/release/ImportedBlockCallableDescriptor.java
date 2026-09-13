package org.pipelineframework.orchestrator.release;

import java.util.Map;
import java.util.Objects;

/** Shared sanitized compile-time resolution of one callable exposed by an imported Block. */
public record ImportedBlockCallableDescriptor(
    String sourceStep,
    String alias,
    String requirement,
    String kind,
    String binding,
    String provider,
    int providerVersion,
    String operation,
    int operationVersion,
    String inputType,
    String outputType,
    Map<String, String> trustedArguments,
    String commandIdGenerator,
    String duplicatePolicy,
    Map<String, Object> commandPolicy,
    String connectorConfigurationDigest
) {
    public ImportedBlockCallableDescriptor {
        trustedArguments = Map.copyOf(Objects.requireNonNull(
            trustedArguments, "trusted arguments must not be null"));
        commandPolicy = commandPolicy == null ? Map.of() : Map.copyOf(commandPolicy);
        connectorConfigurationDigest = connectorConfigurationDigest == null ? "" : connectorConfigurationDigest;
    }
}
