package org.pipelineframework.connector;

import java.util.Collection;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.pipelineframework.command.CommandConnector;
import org.pipelineframework.command.LegacyCommandConnectorProvider;

/**
 * Quarkus/CDI lifecycle adapter. Provider implementations still use only the host-neutral core SPI.
 */
@ApplicationScoped
public class ConnectorRegistryLifecycle {
    @Inject
    Instance<ConnectorProvider<?>> providerInstances;

    @Inject
    Instance<CommandConnector<?, ?>> legacyCommandConnectors;

    @Inject
    ConnectorRuntimeContext runtimeContext;

    private ConnectorRegistry registry;

    void onStart(@Observes StartupEvent event) {
        registry = createRegistry(providerInstances.stream().toList(), legacyCommandConnectors.stream().toList());
        registry.start(runtimeContext).toCompletableFuture().join();
    }

    void onStop(@Observes ShutdownEvent event) {
        if (registry != null) {
            registry.stop(runtimeContext).toCompletableFuture().join();
        }
    }

    @Produces
    @ApplicationScoped
    ConnectorRegistry registry() {
        if (registry == null) {
            throw new IllegalStateException("connector registry is not available before Quarkus startup");
        }
        return registry;
    }

    public static ConnectorRegistry createRegistry(Collection<? extends ConnectorProvider<?>> providers) {
        return new ConnectorRegistry(providers);
    }

    public static ConnectorRegistry createRegistry(
        Collection<? extends ConnectorProvider<?>> providers,
        Collection<? extends CommandConnector<?, ?>> legacyConnectors
    ) {
        return LegacyCommandConnectorProvider.createRegistry(providers, legacyConnectors);
    }
}
