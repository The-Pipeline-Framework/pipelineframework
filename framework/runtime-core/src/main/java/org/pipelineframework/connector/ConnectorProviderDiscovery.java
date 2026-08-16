package org.pipelineframework.connector;

import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * Host-neutral provider construction through explicit instances or {@link ServiceLoader}.
 */
public final class ConnectorProviderDiscovery {
    private ConnectorProviderDiscovery() {
    }

    public static List<ConnectorProvider<?>> discover(ClassLoader classLoader) {
        Objects.requireNonNull(classLoader, "class loader must not be null");
        return ServiceLoader.load(ConnectorProvider.class, classLoader)
            .stream()
            .sorted((left, right) -> left.type().getName().compareTo(right.type().getName()))
            .map(ConnectorProviderDiscovery::provider)
            .toList();
    }

    private static ConnectorProvider<?> provider(ServiceLoader.Provider<ConnectorProvider> service) {
        return Objects.requireNonNull(service.get(), "provider service returned null");
    }
}
