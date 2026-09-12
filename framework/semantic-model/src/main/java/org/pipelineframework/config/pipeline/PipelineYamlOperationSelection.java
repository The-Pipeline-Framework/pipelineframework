package org.pipelineframework.config.pipeline;

import java.util.Map;
import java.util.Objects;

import org.pipelineframework.connector.ConnectorBindingName;

/**
 * Operation-first native connector selection authored on a Command or Query step.
 */
public record PipelineYamlOperationSelection(
    String operation,
    int operationVersion,
    String using,
    Map<String, Object> policy
) {
    public PipelineYamlOperationSelection {
        operation = Objects.requireNonNull(operation, "connector operation ID must not be null").trim();
        if (!operation.matches("[a-z][a-z0-9]*(?:\\.[a-z][a-z0-9]*)*")) {
            throw new IllegalArgumentException("connector operation ID must be a lowercase dotted name: " + operation);
        }
        if (operationVersion < 1) {
            throw new IllegalArgumentException("connector operation version must be positive");
        }
        using = ConnectorBindingName.of(using).value();
        policy = policy == null ? Map.of() : Map.copyOf(policy);
    }
}
