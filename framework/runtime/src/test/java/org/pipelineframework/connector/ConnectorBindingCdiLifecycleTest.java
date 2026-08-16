package org.pipelineframework.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import jakarta.inject.Inject;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandDuplicatePolicy;
import org.pipelineframework.command.CommandRequest;
import org.pipelineframework.command.CommandStepSupport;
import org.pipelineframework.command.InMemoryCommandEffectStore;
import org.pipelineframework.command.NativeCommandSelector;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

@QuarkusTest
@TestProfile(ConnectorBindingCdiLifecycleTest.ConnectorProfile.class)
class ConnectorBindingCdiLifecycleTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    @Inject
    ConnectorRegistry connectorRegistry;

    @Inject
    ConnectorBindingRegistry bindingRegistry;

    @Inject
    ConnectorRuntimeContext runtimeContext;

    @Inject
    InMemoryCommandEffectStore effectStore;

    @Test
    void ownsInjectedProvidersPerBindingAndKeepsReplayAndShutdownLazy() {
        assertEquals(3, bindingRegistry.providers().size());
        assertTrue(bindingRegistry.providerInstances().isEmpty());
        assertEquals(0, InjectedConnectorProvider.unconfiguredStarts());
        assertEquals(0, InjectedConnectorProvider.configurationBindings("first"));

        CommandStepSupport commands = new CommandStepSupport(
            connectorRegistry,
            bindingRegistry,
            List.of(effectStore),
            queueAsyncConfig());

        CommandDescriptor firstOperation = descriptor("cdi-first", "inspect.first");
        AwaitExecutionContext execution = new AwaitExecutionContext("connector-tenant", "connector-execution", 0);
        String replayId = "connector-replay-only";
        CommandRequest<String> replayRequest = new CommandRequest<>(
            firstOperation, replayId, "ignored", execution, firstOperation.config());
        effectStore.createPending(replayRequest, System.currentTimeMillis()).await().atMost(TIMEOUT);
        effectStore.markSucceeded(
            execution.tenantId(), replayId, "recorded", System.currentTimeMillis()).await().atMost(TIMEOUT);

        AwaitExecutionContextHolder.set(execution);
        try {
            String replayed = commands.<String, String>execute(
                firstOperation, (descriptor, input) -> replayId, "ignored").await().atMost(TIMEOUT);
            assertEquals("recorded", replayed);
        } finally {
            AwaitExecutionContextHolder.clear();
        }
        assertTrue(bindingRegistry.providerInstances().isEmpty());
        assertEquals(0, InjectedConnectorProvider.configurationBindings("first"));

        InjectedConnectorProvider.InvocationResult first = invoke(
            commands, execution, "cdi-first", "inspect.first", "live-first", "one");
        assertEquals("injected", first.injection());
        assertEquals("first", first.binding());
        assertEquals(1, bindingRegistry.providerInstances().size());
        assertEquals(1, InjectedConnectorProvider.configurationBindings("first"));
        assertEquals(1, InjectedConnectorProvider.starts(first.providerInstance()));

        InjectedConnectorProvider.InvocationResult shared = invoke(
            commands, execution, "cdi-first", "inspect.second", "live-shared", "two");
        assertEquals(first.providerInstance(), shared.providerInstance());
        assertEquals(1, InjectedConnectorProvider.configurationBindings("first"));
        assertEquals(1, InjectedConnectorProvider.starts(first.providerInstance()));

        InjectedConnectorProvider.InvocationResult second = invoke(
            commands, execution, "cdi-second", "inspect.first", "live-second", "three");
        assertNotEquals(first.providerInstance(), second.providerInstance());
        assertEquals(2, bindingRegistry.providerInstances().size());
        assertEquals(1, InjectedConnectorProvider.configurationBindings("second"));
        assertEquals(1, InjectedConnectorProvider.starts(second.providerInstance()));
        assertEquals(0, InjectedConnectorProvider.unconfiguredStarts());

        CompletionStage<Void> racing = bindingRegistry.activate(
            ConnectorBindingName.of("cdi-racing"), runtimeContext);
        CompletionStage<Void> stopped = bindingRegistry.stop(runtimeContext);
        assertFalse(stopped.toCompletableFuture().isDone());
        RuntimeException rejected = assertThrows(RuntimeException.class, () -> bindingRegistry.activate(
            ConnectorBindingName.of("cdi-first"), runtimeContext).toCompletableFuture().join());
        assertTrue(rejected.getCause().getMessage().contains("shutdown has begun"), rejected.getMessage());

        InjectedConnectorProvider.releaseRacingStart();
        racing.toCompletableFuture().join();
        stopped.toCompletableFuture().join();
        int racingInstance = InjectedConnectorProvider.instanceFor("racing");
        assertEquals(1, InjectedConnectorProvider.configurationBindings("racing"));
        assertEquals(1, InjectedConnectorProvider.starts(racingInstance));
        assertEquals(1, InjectedConnectorProvider.stops(first.providerInstance()));
        assertEquals(1, InjectedConnectorProvider.stops(second.providerInstance()));
        assertEquals(1, InjectedConnectorProvider.stops(racingInstance));
        assertSame(stopped, bindingRegistry.stop(runtimeContext));
        assertEquals(1, InjectedConnectorProvider.stops(first.providerInstance()));
        assertEquals(1, InjectedConnectorProvider.stops(second.providerInstance()));
        assertEquals(1, InjectedConnectorProvider.stops(racingInstance));
        assertEquals(0, InjectedConnectorProvider.unconfiguredStarts());
    }

    private static InjectedConnectorProvider.InvocationResult invoke(
        CommandStepSupport commands,
        AwaitExecutionContext execution,
        String binding,
        String operation,
        String commandId,
        String suffix
    ) {
        AwaitExecutionContextHolder.set(execution);
        try {
            return commands.<String, InjectedConnectorProvider.InvocationResult>execute(
                descriptor(binding, operation), (descriptor, input) -> commandId, "input")
                .await().atMost(TIMEOUT);
        } finally {
            AwaitExecutionContextHolder.clear();
        }
    }

    private static CommandDescriptor descriptor(String binding, String operation) {
        NativeCommandSelector selector = new NativeCommandSelector(
            Optional.of(ConnectorBindingName.of(binding)),
            new ConnectorOperationIdentity(
                ConnectorProviderId.of("test.cdi"), operation, ConnectorOperationKind.COMMAND, 1),
            1,
            CommandPolicy.none());
        return CommandDescriptor.nativeCommand(
            "ConnectorCdiLifecycle-" + binding + "-" + operation,
            selector,
            String.class.getName(),
            InjectedConnectorProvider.InvocationResult.class.getName(),
            "test",
            CommandDuplicatePolicy.RETURN_RECORDED,
            Map.of("suffix", "bound"));
    }

    private static PipelineOrchestratorConfig queueAsyncConfig() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        return config;
    }

    public static final class ConnectorProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("connector.cdi.lifecycle.test", "true");
        }
    }

}
