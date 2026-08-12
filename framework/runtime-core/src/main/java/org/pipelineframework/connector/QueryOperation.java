package org.pipelineframework.connector;

import java.util.concurrent.CompletionStage;
import java.util.Optional;

/**
 * Query-family operation contract. Query outcome semantics are defined by the query runtime work.
 */
public interface QueryOperation<I, C, O> extends ConnectorOperation {
    default Optional<ConnectorConfigSchema<C>> configurationSchema() {
        return Optional.empty();
    }

    CompletionStage<QueryOutcome<O>> query(QueryInvocation<I, C> invocation);

    default CompletionStage<QueryOutcome<O>> query(
        I input,
        ConnectorConfigurationDocument configuration,
        ConnectorExecutionContext executionContext
    ) {
        ConnectorConfigSchema<C> schema = configurationSchema().orElseThrow(() -> new ConnectorConfigurationException(
            "query operation " + descriptor().id() + " does not declare a configuration schema"));
        C boundConfiguration = ConnectorConfigurationBinder.bind(schema, configuration, "query operation " + descriptor().id());
        return query(new QueryInvocation<>(input, boundConfiguration, executionContext));
    }
}
