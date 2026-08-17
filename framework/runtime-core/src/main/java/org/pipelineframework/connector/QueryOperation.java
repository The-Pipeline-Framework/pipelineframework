package org.pipelineframework.connector;

import java.util.concurrent.CompletionStage;
import java.util.Optional;

/**
 * Host-neutral unary Query operation contract.
 *
 * <p>Operations without a configuration schema use {@link ConnectorConfigurationDocument} as
 * {@code C} and receive the empty document.
 */
public interface QueryOperation<I, C, O> extends ConnectorOperation {
    default QueryCapabilities capabilities() {
        return QueryCapabilities.conservative();
    }

    default Optional<ConnectorConfigSchema<C>> configurationSchema() {
        return Optional.empty();
    }

    CompletionStage<QueryOutcome<O>> query(QueryInvocation<I, C, O> invocation);

    default CompletionStage<QueryOutcome<O>> query(
        I input,
        ConnectorConfigurationDocument configuration,
        Class<O> outputType,
        ConnectorExecutionContext executionContext
    ) {
        C boundConfiguration = configurationSchema()
            .map(schema -> ConnectorConfigurationBinder.bind(schema, configuration, "query operation " + id()))
            .orElseGet(() -> zeroConfiguration(configuration));
        return query(new QueryInvocation<>(input, boundConfiguration, outputType, executionContext));
    }

    @SuppressWarnings("unchecked")
    private C zeroConfiguration(ConnectorConfigurationDocument configuration) {
        if (!configuration.values().isEmpty()) {
            throw new ConnectorConfigurationException(
                "query operation " + id() + " does not declare a configuration schema");
        }
        return (C) ConnectorConfigurationDocument.empty();
    }
}
