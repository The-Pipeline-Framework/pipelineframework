package org.pipelineframework.connector;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConnectorRegistryLifecycleTest {

    @Test
    void cdiAdapterBuildsTheSameHostNeutralRegistryFromAnExplicitProviderCollection() {
        ConnectorRegistry registry = ConnectorRegistryLifecycle.createRegistry(List.of(new TestProvider()));

        assertEquals("cdi.adapter", registry.providers().keySet().iterator().next().value());
    }

    private static final class TestProvider implements ConnectorProvider<Void> {
        @Override
        public ConnectorProviderDescriptor descriptor() {
            return new ConnectorProviderDescriptor(ConnectorProviderId.of("cdi.adapter"), new ConnectorProviderVersion(1, 0));
        }

        @Override
        public Collection<? extends ConnectorOperation> operations() {
            return List.of(() -> new ConnectorOperationDescriptor("lookup", ConnectorOperationKind.QUERY, 1));
        }

        @Override
        public CompletionStage<Void> start(ConnectorRuntimeContext context) {
            return ConnectorCompletionStages.completed();
        }

        @Override
        public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
            return ConnectorCompletionStages.completed();
        }
    }
}
