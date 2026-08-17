package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.connector.CommandOperation;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderDescriptor;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRegistry;
import org.pipelineframework.connector.ConnectorRuntimeContext;

class LegacyCommandConnectorProviderTest {

    @Test
    void dispatchesTheUnchangedLegacyRequestThroughTheRegistry() {
        RecordingConnector connector = new RecordingConnector("observed-operation", Uni.createFrom().item("done"));
        CommandRequest<String> request = request("observed-operation", Map.of("route", "primary"));
        ConnectorRegistry registry = LegacyCommandConnectorProvider.createRegistry(List.of(), List.of(connector));

        String output = await(LegacyCommandConnectorProvider.dispatch(registry, request));

        assertEquals("done", output);
        assertSame(request, connector.request.get());
        ConnectorOperation operation = registry.operations().values().iterator().next();
        assertEquals("legacy.observed.operation", operation.id());
        ConnectorOperationIdentity identity = registry.operations().keySet().iterator().next();
        assertInstanceOf(CommandOperation.class, registry.requireExecutionOperation(identity, CommandOperation.class));
    }

    @Test
    void assignsReadableIdentitiesIndependentlyOfConnectorDiscoveryOrder() {
        List<CommandConnector<?, ?>> forward = List.of(
            new RecordingConnector("alpha.command", Uni.createFrom().item("alpha")),
            new RecordingConnector("hyphen-command", Uni.createFrom().item("hyphen")),
            new RecordingConnector("invalid/command", Uni.createFrom().item("invalid")),
            new RecordingConnector("collision-command", Uni.createFrom().item("left")),
            new RecordingConnector("collision.command", Uni.createFrom().item("right")));
        List<CommandConnector<?, ?>> reverse = List.of(
            forward.get(4), forward.get(3), forward.get(2), forward.get(1), forward.get(0));

        List<String> forwardIds = operationIds(LegacyCommandConnectorProvider.from(forward).orElseThrow());
        List<String> reverseIds = operationIds(LegacyCommandConnectorProvider.from(reverse).orElseThrow());

        assertEquals(forwardIds, reverseIds);
        assertTrue(forwardIds.contains("legacy.alpha.command"));
        assertTrue(forwardIds.contains("legacy.hyphen.command"));
        assertTrue(forwardIds.contains("legacy.command.c696e76616c69642f636f6d6d616e64"));
        assertTrue(forwardIds.contains("legacy.collision.command.c636f6c6c6973696f6e2d636f6d6d616e64"));
        assertTrue(forwardIds.contains("legacy.collision.command.c636f6c6c6973696f6e2e636f6d6d616e64"));
    }

    @Test
    void rejectsDuplicateLegacyCommandsAndFrameworkNamespaceCollisionsDeterministically() {
        IllegalArgumentException duplicate = assertThrows(IllegalArgumentException.class,
            () -> LegacyCommandConnectorProvider.from(List.of(new DuplicateA(), new DuplicateB())));
        assertEquals("duplicate legacy CommandConnector command 'duplicate': "
            + DuplicateA.class.getName() + " and " + DuplicateB.class.getName(), duplicate.getMessage());

        IllegalArgumentException collision = assertThrows(IllegalArgumentException.class,
            () -> LegacyCommandConnectorProvider.createRegistry(List.of(new ReservedProvider()),
                List.of(new RecordingConnector("legacy", Uni.createFrom().item("value")))));
        assertEquals("duplicate connector provider ID: tpf.legacy.command", collision.getMessage());
    }

    @Test
    void preservesOriginalFailuresAndRejectsNullUnis() {
        IllegalStateException thrown = new IllegalStateException("thrown");
        RecordingConnector throwing = new RecordingConnector("throws", null) {
            @Override
            public Uni<String> execute(CommandRequest<String> request) {
                throw thrown;
            }
        };
        IllegalArgumentException failed = new IllegalArgumentException("failed");
        RecordingConnector failing = new RecordingConnector("fails", Uni.createFrom().failure(failed));
        RecordingConnector nullUni = new RecordingConnector("null-uni", null);

        assertSame(thrown, failure(LegacyCommandConnectorProvider.dispatch(
            LegacyCommandConnectorProvider.createRegistry(List.of(), List.of(throwing)), request("throws", Map.of()))));
        assertSame(failed, failure(LegacyCommandConnectorProvider.dispatch(
            LegacyCommandConnectorProvider.createRegistry(List.of(), List.of(failing)), request("fails", Map.of()))));
        Throwable nullFailure = failure(LegacyCommandConnectorProvider.dispatch(
            LegacyCommandConnectorProvider.createRegistry(List.of(), List.of(nullUni)), request("null-uni", Map.of())));
        assertInstanceOf(IllegalStateException.class, nullFailure);
        assertTrue(nullFailure.getMessage().contains("returned null Uni"));
    }

    @Test
    void cancellationCancelsTheMutinySubscription() throws InterruptedException {
        CountDownLatch terminated = new CountDownLatch(1);
        RecordingConnector connector = new RecordingConnector("cancel", Uni.createFrom().emitter(emitter ->
            emitter.onTermination(terminated::countDown)));
        CompletionStage<String> stage = LegacyCommandConnectorProvider.dispatch(
            LegacyCommandConnectorProvider.createRegistry(List.of(), List.of(connector)), request("cancel", Map.of()));

        assertTrue(stage.toCompletableFuture().cancel(true));
        assertTrue(stage.toCompletableFuture().isCancelled());
        assertTrue(terminated.await(2, TimeUnit.SECONDS));
    }

    private static List<String> operationIds(LegacyCommandConnectorProvider provider) {
        return provider.operations().stream().map(ConnectorOperation::id).sorted().toList();
    }

    private static CommandRequest<String> request(String command, Map<String, Object> config) {
        CommandDescriptor descriptor = new CommandDescriptor(
            "step-1", command, String.class.getName(), String.class.getName(), "test.generator",
            CommandDuplicatePolicy.RETURN_RECORDED, config);
        return new CommandRequest<>(
            descriptor, "command-1", "input", new AwaitExecutionContext("tenant", "execution", 3), config);
    }

    private static <T> T await(CompletionStage<T> stage) {
        return stage.toCompletableFuture().orTimeout(Duration.ofSeconds(2).toMillis(), TimeUnit.MILLISECONDS).join();
    }

    private static Throwable failure(CompletionStage<?> stage) {
        AtomicReference<Throwable> observed = new AtomicReference<>();
        stage.whenComplete((ignored, failure) -> observed.set(failure));
        assertThrows(Exception.class, () -> stage.toCompletableFuture().get(2, TimeUnit.SECONDS));
        return observed.get();
    }

    private static class RecordingConnector implements CommandConnector<String, String> {
        private final String command;
        private final Uni<String> result;
        private final AtomicReference<CommandRequest<String>> request = new AtomicReference<>();

        private RecordingConnector(String command, Uni<String> result) {
            this.command = command;
            this.result = result;
        }

        @Override
        public String command() {
            return command;
        }

        @Override
        public Uni<String> execute(CommandRequest<String> request) {
            this.request.set(request);
            return result;
        }
    }

    private static final class DuplicateA extends RecordingConnector {
        private DuplicateA() {
            super("duplicate", Uni.createFrom().item("a"));
        }
    }

    private static final class DuplicateB extends RecordingConnector {
        private DuplicateB() {
            super("duplicate", Uni.createFrom().item("b"));
        }
    }

    private static final class ReservedProvider implements ConnectorProvider<Void> {
        @Override
        public ConnectorProviderId id() {
            return ConnectorProviderId.of("tpf.legacy.command");
        }

        @Override
        public ConnectorProviderVersion version() {
            return new ConnectorProviderVersion(1, 0);
        }

        @Override
        public List<? extends ConnectorOperation> operations() {
            return List.of();
        }

    }
}
