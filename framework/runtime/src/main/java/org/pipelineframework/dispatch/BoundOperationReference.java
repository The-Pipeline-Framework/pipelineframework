package org.pipelineframework.dispatch;

import java.util.Objects;

import org.pipelineframework.connector.ConnectorBindingName;

/** Binding-aware identity selected by an inert AgentCall proposal. */
public record BoundOperationReference(ConnectorBindingName binding, String operation) {
    public BoundOperationReference {
        binding = Objects.requireNonNull(binding, "connector binding must not be null");
        operation = Objects.requireNonNull(operation, "connector operation must not be null").trim();
        if (operation.isEmpty()) {
            throw new IllegalArgumentException("connector operation must not be blank");
        }
    }
}
