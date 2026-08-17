package org.pipelineframework.connector;

import java.util.Objects;

/**
 * Query-family invocation, keeping operation configuration separate from dynamic input.
 */
public record QueryInvocation<I, C, O>(
    I input,
    C configuration,
    Class<O> outputType,
    ConnectorExecutionContext executionContext
) {
    public QueryInvocation {
        input = Objects.requireNonNull(input, "query input must not be null");
        configuration = Objects.requireNonNull(configuration, "query configuration must not be null");
        outputType = Objects.requireNonNull(outputType, "query output type must not be null");
        executionContext = Objects.requireNonNull(executionContext, "execution context must not be null");
    }
}
