package org.pipelineframework.connector;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

/**
 * Reusable provider packaging and lifecycle unit. Its configuration type is provider-owned.
 */
public interface ConnectorProvider<PC> {
    ConnectorProviderDescriptor descriptor();

    Collection<? extends ConnectorOperation> operations();

    CompletionStage<Void> start(ConnectorRuntimeContext context);

    CompletionStage<Void> stop(ConnectorRuntimeContext context);
}
