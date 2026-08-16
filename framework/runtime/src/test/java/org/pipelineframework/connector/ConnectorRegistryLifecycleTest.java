package org.pipelineframework.connector;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConnectorRegistryLifecycleTest {

    @BeforeEach
    void resetStarts() {
        TestProvider.starts.set(0);
    }

    @Test
    void cdiAdapterBuildsTheSameHostNeutralRegistryFromAnExplicitProviderCollection() {
        ConnectorRegistry registry = ConnectorRegistryLifecycle.createRegistry(List.of(new TestProvider()));

        assertEquals("cdi.adapter", registry.providers().keySet().iterator().next().value());
    }

    @Test
    void cdiAdapterCreatesALazyNamedBindingAndActivatesItOnce() {
        ConnectorBindingRegistry bindings = ConnectorRegistryLifecycle.createBindingRegistry(
            List.of(new ConnectorBindingDefinition(
                ConnectorBindingName.of("shared"),
                ConnectorProviderId.of("cdi.adapter"),
                1,
                new ConnectorConfigurationDocument(Map.of()))),
            List.of(new TestProvider()));

        assertEquals(0, TestProvider.starts.get());

        bindings.activate(ConnectorBindingName.of("shared"), ConnectorRuntimeContext.empty())
            .toCompletableFuture().join();
        bindings.activate(ConnectorBindingName.of("shared"), ConnectorRuntimeContext.empty())
            .toCompletableFuture().join();

        assertEquals("lookup", bindings.requireOperation(
            ConnectorBindingName.of("shared"), "lookup", ConnectorOperationKind.QUERY, 1).id());
        assertEquals(1, TestProvider.starts.get());
    }

    public static final class TestProvider implements ConnectorProvider<Void> {
        private static final AtomicInteger starts = new AtomicInteger();

        public TestProvider() {
        }

        @Override
        public ConnectorProviderId id() {
            return ConnectorProviderId.of("cdi.adapter");
        }

        @Override
        public ConnectorProviderVersion version() {
            return new ConnectorProviderVersion(1, 0);
        }

        @Override
        public Collection<? extends ConnectorOperation> operations() {
            return List.of(new QueryOperation<Object, Object, Object>() {
                @Override
                public String id() {
                    return "lookup";
                }

                @Override
                public CompletionStage<QueryOutcome<Object>> query(QueryInvocation<Object, Object, Object> invocation) {
                    return CompletableFuture.failedFuture(new UnsupportedOperationException("not invoked"));
                }
            });
        }

        @Override
        public CompletionStage<Void> start(ConnectorRuntimeContext context) {
            starts.incrementAndGet();
            return ConnectorCompletionStages.completed();
        }
    }
}
