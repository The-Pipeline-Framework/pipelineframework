package org.pipelineframework.connector;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/** Test access to the host-internal binding instance seam. */
public final class TestConnectorBindingRegistries {
    private TestConnectorBindingRegistries() {
    }

    public static ConnectorBindingRegistry fromProviderSupplier(
        Collection<ConnectorBindingDefinition> definitions,
        ConnectorProvider<?> prototype,
        Supplier<? extends ConnectorProvider<?>> providerSupplier
    ) {
        return ConnectorBindingRegistry.fromProviders(
            definitions,
            List.of(prototype),
            ignored -> ConnectorProviderLease.of(providerSupplier.get()));
    }
}
