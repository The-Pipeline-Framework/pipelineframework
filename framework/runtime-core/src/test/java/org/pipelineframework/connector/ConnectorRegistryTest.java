package org.pipelineframework.connector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectorRegistryTest {

    @Test
    void startsInStableIdentityOrderAndStopsInReverseUsingCompletionStages() {
        List<String> lifecycle = new ArrayList<>();
        ConnectorRegistry registry = new ConnectorRegistry(List.of(
            provider("zulu.provider", lifecycle, operation("write", ConnectorOperationKind.COMMAND)),
            provider("alpha.provider", lifecycle, operation("read", ConnectorOperationKind.QUERY))));

        CompletionStage<Void> started = registry.start(ConnectorRuntimeContext.empty());
        started.toCompletableFuture().join();
        CompletionStage<Void> stopped = registry.stop(ConnectorRuntimeContext.empty());
        stopped.toCompletableFuture().join();

        assertEquals(List.of("start:alpha.provider", "start:zulu.provider", "stop:zulu.provider", "stop:alpha.provider"), lifecycle);
    }

    @Test
    void rejectsDuplicateProviderAndOperationIdentitiesDeterministically() {
        IllegalArgumentException duplicateProvider = assertThrows(IllegalArgumentException.class, () -> new ConnectorRegistry(List.of(
            provider("duplicate.provider", new ArrayList<>(), operation("first", ConnectorOperationKind.COMMAND)),
            provider("duplicate.provider", new ArrayList<>(), operation("second", ConnectorOperationKind.QUERY)))));
        assertEquals("duplicate connector provider ID: duplicate.provider", duplicateProvider.getMessage());

        ConnectorOperation duplicate = operation("same", ConnectorOperationKind.COMMAND);
        IllegalArgumentException duplicateOperation = assertThrows(IllegalArgumentException.class, () -> new ConnectorRegistry(List.of(
            provider("operation.provider", new ArrayList<>(), List.of(duplicate, duplicate)))));
        assertTrue(duplicateOperation.getMessage().startsWith("duplicate connector operation identity:"));
    }

    @Test
    void validatesExactProviderMajorVersionAndMalformedDescriptors() {
        ConnectorRegistry registry = new ConnectorRegistry(List.of(
            provider("versioned.provider", new ArrayList<>(), operation("read", ConnectorOperationKind.QUERY))));

        IllegalStateException incompatible = assertThrows(
            IllegalStateException.class, () -> registry.requireProvider(ConnectorProviderId.of("versioned.provider"), 2));
        assertTrue(incompatible.getMessage().contains("exact-major compatibility"));
        assertThrows(IllegalArgumentException.class, () -> ConnectorProviderId.of("Invalid.Provider"));
        assertThrows(IllegalArgumentException.class, () -> ConnectorOperationKind.of("not-namespaced"));
    }

    @Test
    void registersReservedAgentMetadataButRejectsItsExecutionPath() {
        AgentOperation agent = new AgentOperation() {
            @Override
            public ConnectorOperationDescriptor descriptor() {
                return new ConnectorOperationDescriptor("review", ConnectorOperationKind.AGENT, 1);
            }
        };
        ConnectorRegistry registry = new ConnectorRegistry(List.of(provider("agent.provider", new ArrayList<>(), agent)));
        ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
            ConnectorProviderId.of("agent.provider"), "review", ConnectorOperationKind.AGENT, 1);

        assertInstanceOf(AgentOperation.class, registry.requireOperation(identity));
        UnsupportedOperationException exception = assertThrows(
            UnsupportedOperationException.class, () -> registry.requireExecutionOperation(identity, AgentOperation.class));
        assertTrue(exception.getMessage().contains("metadata only"));
    }

    @Test
    void reservesTheTpfNamespaceUnlessAnExplicitFrameworkAllowlistAdmitsTheProvider() {
        ConnectorProvider<Void> frameworkProvider = provider(
            "tpf.legacy.command", new ArrayList<>(), operation("legacy.command", ConnectorOperationKind.COMMAND));

        IllegalArgumentException rejected = assertThrows(
            IllegalArgumentException.class, () -> new ConnectorRegistry(List.of(frameworkProvider)));
        assertEquals("connector provider ID is reserved for framework use: tpf.legacy.command", rejected.getMessage());

        ConnectorRegistry allowed = ConnectorRegistry.withFrameworkProviders(
            List.of(frameworkProvider), List.of(ConnectorProviderId.of("tpf.legacy.command")));
        assertEquals(1, allowed.providers().size());
    }

    @Test
    void continuesStoppingAfterFailureAndRetriesOnlyProvidersThatDidNotStop() {
        List<String> lifecycle = new ArrayList<>();
        AtomicBoolean failZuluStop = new AtomicBoolean(true);
        ConnectorRegistry registry = new ConnectorRegistry(List.of(
            providerWithStop("alpha.provider", lifecycle, () -> ConnectorCompletionStages.completed()),
            providerWithStop("zulu.provider", lifecycle, () -> failZuluStop.getAndSet(false)
                ? CompletableFuture.failedFuture(new IllegalStateException("zulu unavailable"))
                : ConnectorCompletionStages.completed())));

        registry.start(ConnectorRuntimeContext.empty()).toCompletableFuture().join();
        assertThrows(RuntimeException.class, () -> registry.stop(ConnectorRuntimeContext.empty()).toCompletableFuture().join());
        assertEquals(List.of("start:alpha.provider", "start:zulu.provider", "stop:zulu.provider", "stop:alpha.provider"), lifecycle);

        registry.stop(ConnectorRuntimeContext.empty()).toCompletableFuture().join();
        assertEquals(List.of("start:alpha.provider", "start:zulu.provider", "stop:zulu.provider", "stop:alpha.provider", "stop:zulu.provider"), lifecycle);
    }

    @Test
    void ignoresLateStartCompletionAfterStopHasBegun() {
        CompletableFuture<Void> startGate = new CompletableFuture<>();
        CompletableFuture<Void> stopGate = new CompletableFuture<>();
        AtomicInteger stopInvocations = new AtomicInteger();
        ConnectorRegistry registry = new ConnectorRegistry(List.of(new ConnectorProvider<Void>() {
            @Override
            public ConnectorProviderDescriptor descriptor() {
                return new ConnectorProviderDescriptor(ConnectorProviderId.of("gated.provider"), new ConnectorProviderVersion(1, 0));
            }

            @Override
            public Collection<? extends ConnectorOperation> operations() {
                return List.of();
            }

            @Override
            public CompletionStage<Void> start(ConnectorRuntimeContext context) {
                return startGate;
            }

            @Override
            public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
                stopInvocations.incrementAndGet();
                return stopGate;
            }
        }));

        CompletionStage<Void> started = registry.start(ConnectorRuntimeContext.empty());
        CompletionStage<Void> stopped = registry.stop(ConnectorRuntimeContext.empty());
        startGate.complete(null);

        assertEquals(1, stopInvocations.get());
        assertEquals(stopped, registry.stop(ConnectorRuntimeContext.empty()));
        assertEquals(1, stopInvocations.get());

        stopGate.complete(null);
        started.toCompletableFuture().join();
        stopped.toCompletableFuture().join();
        assertEquals(1, stopInvocations.get());
    }

    private static ConnectorProvider<Void> provider(
        String id,
        List<String> lifecycle,
        ConnectorOperation operation
    ) {
        return provider(id, lifecycle, List.of(operation));
    }

    private static ConnectorProvider<Void> provider(
        String id,
        List<String> lifecycle,
        Collection<? extends ConnectorOperation> operations
    ) {
        return new ConnectorProvider<>() {
            @Override
            public ConnectorProviderDescriptor descriptor() {
                return new ConnectorProviderDescriptor(ConnectorProviderId.of(id), new ConnectorProviderVersion(1, 0));
            }

            @Override
            public Collection<? extends ConnectorOperation> operations() {
                return operations;
            }

            @Override
            public CompletionStage<Void> start(ConnectorRuntimeContext context) {
                return CompletableFuture.runAsync(() -> lifecycle.add("start:" + id));
            }

            @Override
            public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
                return CompletableFuture.runAsync(() -> lifecycle.add("stop:" + id));
            }
        };
    }

    private static ConnectorOperation operation(String id, ConnectorOperationKind kind) {
        return () -> new ConnectorOperationDescriptor(id, kind, 1);
    }

    private static ConnectorProvider<Void> providerWithStop(
        String id,
        List<String> lifecycle,
        java.util.function.Supplier<CompletionStage<Void>> stop
    ) {
        return new ConnectorProvider<>() {
            @Override
            public ConnectorProviderDescriptor descriptor() {
                return new ConnectorProviderDescriptor(ConnectorProviderId.of(id), new ConnectorProviderVersion(1, 0));
            }

            @Override
            public Collection<? extends ConnectorOperation> operations() {
                return List.of();
            }

            @Override
            public CompletionStage<Void> start(ConnectorRuntimeContext context) {
                lifecycle.add("start:" + id);
                return ConnectorCompletionStages.completed();
            }

            @Override
            public CompletionStage<Void> stop(ConnectorRuntimeContext context) {
                lifecycle.add("stop:" + id);
                return stop.get();
            }
        };
    }
}
