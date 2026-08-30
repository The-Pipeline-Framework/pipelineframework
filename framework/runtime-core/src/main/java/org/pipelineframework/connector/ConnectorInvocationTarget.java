package org.pipelineframework.connector;

import java.util.Objects;

/**
 * Compiler-authorized connector target for one managed operation invocation.
 */
public record ConnectorInvocationTarget(
    ConnectorBindingName bindingName,
    ConnectorOperationIdentity operation
) {
    public ConnectorInvocationTarget {
        bindingName = Objects.requireNonNull(bindingName, "connector binding name must not be null");
        operation = Objects.requireNonNull(operation, "connector operation identity must not be null");
    }
}
