package org.pipelineframework.connector;

import java.util.Objects;
import java.util.Optional;

/**
 * Command-family invocation, keeping operation configuration separate from dynamic input.
 */
public record CommandInvocation<I, C>(
    I input,
    C configuration,
    Class<?> outputType,
    ConnectorExecutionContext executionContext,
    Optional<CommandDispatchIdentity> dispatchIdentity
) {
    public CommandInvocation(
        I input,
        C configuration,
        ConnectorExecutionContext executionContext,
        Optional<CommandDispatchIdentity> dispatchIdentity
    ) {
        this(input, configuration, Object.class, executionContext, dispatchIdentity);
    }

    public CommandInvocation(I input, C configuration, ConnectorExecutionContext executionContext) {
        this(input, configuration, Object.class, executionContext, Optional.empty());
    }

    public CommandInvocation {
        input = Objects.requireNonNull(input, "command input must not be null");
        configuration = Objects.requireNonNull(configuration, "command configuration must not be null");
        outputType = Objects.requireNonNull(outputType, "command output type must not be null");
        executionContext = Objects.requireNonNull(executionContext, "execution context must not be null");
        dispatchIdentity = Objects.requireNonNull(dispatchIdentity, "dispatch identity must not be null");
    }
}
