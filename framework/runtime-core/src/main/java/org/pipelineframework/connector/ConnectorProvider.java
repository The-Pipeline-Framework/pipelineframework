package org.pipelineframework.connector;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Reusable provider packaging and lifecycle unit. Its configuration type is provider-owned.
 */
public interface ConnectorProvider<PC> {
    ConnectorProviderDescriptor descriptor();

    Collection<? extends ConnectorOperation> operations();

    default ConnectorExecutionCapabilities executionCapabilities() {
        return ConnectorExecutionCapabilities.conservative();
    }

    default Optional<ConnectorConfigSchema<PC>> configurationSchema() {
        return Optional.empty();
    }

    CompletionStage<Void> start(ConnectorRuntimeContext context);

    default CompletionStage<Void> start(ConnectorRuntimeContext context, PC configuration) {
        return start(context);
    }

    CompletionStage<Void> stop(ConnectorRuntimeContext context);
}
