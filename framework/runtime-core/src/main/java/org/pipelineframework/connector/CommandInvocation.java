package org.pipelineframework.connector;

import java.util.Objects;

/**
 * Command-family invocation, keeping operation configuration separate from dynamic input.
 */
public record CommandInvocation<I, C>(I input, C configuration, ConnectorExecutionContext executionContext) {
    public CommandInvocation {
        input = Objects.requireNonNull(input, "command input must not be null");
        configuration = Objects.requireNonNull(configuration, "command configuration must not be null");
        executionContext = Objects.requireNonNull(executionContext, "execution context must not be null");
    }
}
