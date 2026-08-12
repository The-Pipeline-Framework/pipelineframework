package org.pipelineframework.connector;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;

/**
 * Host-neutral provider construction through explicit factories or {@link ServiceLoader}.
 */
public final class ConnectorProviderDiscovery {
    private ConnectorProviderDiscovery() {
    }

    public static List<ConnectorProvider<?>> discover(ClassLoader classLoader) {
        Objects.requireNonNull(classLoader, "class loader must not be null");
        List<ConnectorProviderFactory> factories = ServiceLoader.load(ConnectorProviderFactory.class, classLoader)
            .stream()
            .sorted((left, right) -> left.type().getName().compareTo(right.type().getName()))
            .map(ServiceLoader.Provider::get)
            .sorted((left, right) -> left.getClass().getName().compareTo(right.getClass().getName()))
            .toList();
        return fromFactories(factories);
    }

    public static List<ConnectorProvider<?>> fromFactories(Collection<? extends ConnectorProviderFactory> factories) {
        Objects.requireNonNull(factories, "provider factories must not be null");
        List<ConnectorProvider<?>> providers = new java.util.ArrayList<>();
        for (ConnectorProviderFactory factory : factories) {
            ConnectorProviderFactory checkedFactory = Objects.requireNonNull(factory, "provider factory must not be null");
            providers.add(Objects.requireNonNull(checkedFactory.create(), "provider factory returned null"));
        }
        return List.copyOf(providers);
    }
}
