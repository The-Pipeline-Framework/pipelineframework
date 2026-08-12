package org.pipelineframework.connector;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

/**
 * Quarkus/CDI adapter that supplies the provider-lifetime core runtime context.
 */
@ApplicationScoped
public class ConnectorRuntimeContextProducer {
    @Produces
    @ApplicationScoped
    ConnectorRuntimeContext connectorRuntimeContext() {
        return ConnectorRuntimeContext.of(
            "quarkus",
            Runnable::run,
            java.time.Clock.systemUTC(),
            java.util.Optional.empty(),
            java.util.Optional.empty());
    }
}
