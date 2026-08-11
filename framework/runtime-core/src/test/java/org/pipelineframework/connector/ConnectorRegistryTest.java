package org.pipelineframework.connector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
}
