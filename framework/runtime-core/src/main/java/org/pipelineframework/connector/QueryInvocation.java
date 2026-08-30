package org.pipelineframework.connector;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Query-family invocation, keeping operation configuration separate from dynamic input.
 *
 * <p>The optional local result mapper lets an in-process provider convert a provider-owned
 * representation before closing resources that representation may depend on. Providers that do
 * not consume it retain the runtime's post-provider mapping fallback. It is deliberately absent
 * from remote transport contracts.</p>
 */
public record QueryInvocation<I, C, O>(
    I input,
    C configuration,
    Class<O> outputType,
    ConnectorExecutionContext executionContext,
    Optional<PayloadMaterializer> payloadMaterializer,
    Optional<Function<O, ?>> localResultMapper
) {
    public QueryInvocation {
        input = Objects.requireNonNull(input, "query input must not be null");
        configuration = Objects.requireNonNull(configuration, "query configuration must not be null");
        outputType = Objects.requireNonNull(outputType, "query output type must not be null");
        executionContext = Objects.requireNonNull(executionContext, "execution context must not be null");
        payloadMaterializer = Objects.requireNonNull(payloadMaterializer,
            "query payload materializer must not be null");
        localResultMapper = Objects.requireNonNull(localResultMapper,
            "query local result mapper must not be null");
    }

    public QueryInvocation(
        I input,
        C configuration,
        Class<O> outputType,
        ConnectorExecutionContext executionContext,
        Optional<PayloadMaterializer> payloadMaterializer
    ) {
        this(input, configuration, outputType, executionContext, payloadMaterializer, Optional.empty());
    }

    public QueryInvocation(
        I input,
        C configuration,
        Class<O> outputType,
        ConnectorExecutionContext executionContext
    ) {
        this(input, configuration, outputType, executionContext, Optional.empty(), Optional.empty());
    }
}
