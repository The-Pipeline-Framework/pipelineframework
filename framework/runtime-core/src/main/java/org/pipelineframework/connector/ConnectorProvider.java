package org.pipelineframework.connector;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Reusable provider packaging and lifecycle unit. Its configuration type is provider-owned.
 */
public interface ConnectorProvider<PC> {
    ConnectorProviderId id();

    ConnectorProviderVersion version();

    Collection<? extends ConnectorOperation> operations();

    default ConnectorExecutionCapabilities executionCapabilities() {
        return ConnectorExecutionCapabilities.conservative();
    }

    default Optional<ConnectorConfigSchema<PC>> configurationSchema() {
        return Optional.empty();
    }

    default CompletionStage<Void> start(ConnectorRuntimeContext context) {
        return ConnectorCompletionStages.completed();
    }

    default CompletionStage<Void> start(ConnectorRuntimeContext context, PC configuration) {
        return start(context);
    }

    default CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        return ConnectorCompletionStages.completed();
    }
}
