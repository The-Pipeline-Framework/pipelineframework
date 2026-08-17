package org.pipelineframework.connector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    void activationFailuresStayReactiveAndMayBeRetried() {
        ConnectorRegistry empty = new ConnectorRegistry(List.of());
        CompletionStage<Void> missing = empty.activate(
            ConnectorProviderId.of("missing.provider"), ConnectorRuntimeContext.empty());
        assertThrows(RuntimeException.class, () -> missing.toCompletableFuture().join());

        AtomicInteger attempts = new AtomicInteger();
        ConnectorProvider<Void> provider = new ConnectorProvider<>() {
            @Override
            public ConnectorProviderId id() {
                return ConnectorProviderId.of("retry.provider");
            }

            @Override
            public ConnectorProviderVersion version() {
                return new ConnectorProviderVersion(1, 0);
            }

            @Override
            public Collection<? extends ConnectorOperation> operations() {
                return List.of();
            }

            @Override
            public CompletionStage<Void> start(ConnectorRuntimeContext context) {
                return attempts.incrementAndGet() == 1
                    ? CompletableFuture.failedFuture(new IllegalStateException("temporary"))
                    : ConnectorCompletionStages.completed();
            }
        };
        ConnectorRegistry registry = new ConnectorRegistry(List.of(provider));

        assertThrows(RuntimeException.class, () -> registry.activate(
            provider.id(), ConnectorRuntimeContext.empty()).toCompletableFuture().join());
        registry.activate(provider.id(), ConnectorRuntimeContext.empty()).toCompletableFuture().join();

        assertEquals(2, attempts.get());
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
    void describesTheCompleteReservedTpfNamespaceInFrameworkAllowlistDiagnostics() {
        IllegalArgumentException rejected = assertThrows(IllegalArgumentException.class,
            () -> ConnectorRegistry.withFrameworkProviders(List.of(), List.of(ConnectorProviderId.of("application.provider"))));

        assertEquals(
            "framework provider allowlist ID must use the reserved tpf namespace ('tpf' or 'tpf.*'): application.provider",
            rejected.getMessage());
    }

    @Test
    void continuesStoppingAfterFailureAndKeepsShutdownIdempotent() {
        List<String> lifecycle = new ArrayList<>();
        ConnectorRegistry registry = new ConnectorRegistry(List.of(
            providerWithStop("alpha.provider", lifecycle, () -> ConnectorCompletionStages.completed()),
            providerWithStop("zulu.provider", lifecycle,
                () -> CompletableFuture.failedFuture(new IllegalStateException("zulu unavailable")))));

        registry.start(ConnectorRuntimeContext.empty()).toCompletableFuture().join();
        CompletionStage<Void> stopped = registry.stop(ConnectorRuntimeContext.empty());
        assertThrows(RuntimeException.class, () -> stopped.toCompletableFuture().join());
        assertEquals(List.of("start:alpha.provider", "start:zulu.provider", "stop:zulu.provider", "stop:alpha.provider"), lifecycle);

        assertEquals(stopped, registry.stop(ConnectorRuntimeContext.empty()));
        assertThrows(RuntimeException.class, () -> stopped.toCompletableFuture().join());
        assertEquals(List.of("start:alpha.provider", "start:zulu.provider", "stop:zulu.provider", "stop:alpha.provider"), lifecycle);
    }

    @Test
    void ignoresLateStartCompletionAfterStopHasBegun() {
        CompletableFuture<Void> startGate = new CompletableFuture<>();
        CompletableFuture<Void> stopGate = new CompletableFuture<>();
        AtomicInteger stopInvocations = new AtomicInteger();
        ConnectorRegistry registry = new ConnectorRegistry(List.of(new ConnectorProvider<Void>() {
            @Override
            public ConnectorProviderId id() {
                return ConnectorProviderId.of("gated.provider");
            }

            @Override
            public ConnectorProviderVersion version() {
                return new ConnectorProviderVersion(1, 0);
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
            public ConnectorProviderId id() {
                return ConnectorProviderId.of(id);
            }

            @Override
            public ConnectorProviderVersion version() {
                return new ConnectorProviderVersion(1, 0);
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
        if (ConnectorOperationKind.COMMAND.equals(kind)) {
            return new CommandOperation<Object, Object, Object>() {
                @Override
                public String id() {
                    return id;
                }

                @Override
                public CompletionStage<CommandOutcome<Object>> dispatch(CommandInvocation<Object, Object> invocation) {
                    return CompletableFuture.failedFuture(new UnsupportedOperationException("metadata-only test operation"));
                }
            };
        }
        return new QueryOperation<Object, Object, Object>() {
            @Override
            public String id() {
                return id;
            }

            @Override
            public CompletionStage<QueryOutcome<Object>> query(QueryInvocation<Object, Object, Object> invocation) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException("metadata-only test operation"));
            }
        };
    }

    private static ConnectorProvider<Void> providerWithStop(
        String id,
        List<String> lifecycle,
        java.util.function.Supplier<CompletionStage<Void>> stop
    ) {
        return new ConnectorProvider<>() {
            @Override
            public ConnectorProviderId id() {
                return ConnectorProviderId.of(id);
            }

            @Override
            public ConnectorProviderVersion version() {
                return new ConnectorProviderVersion(1, 0);
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
