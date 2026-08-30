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
import jakarta.enterprise.inject.Instance;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
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
    ConnectorRuntimeContext runtimeContext;

    @Inject
    Instance<ConnectorProvider<?>> providerInstances;

    @Inject
    QuarkusConnectorProviderInstanceFactory providerInstanceFactory;

    @BeforeEach
    void resetProviderObservations() {
        InjectedConnectorProvider.resetObservations();
    }

    @Test
    void startupAndReplayDoNotActivateConfiguredProviders() {
        Fixture fixture = fixture("cdi-first");
        assertTrue(fixture.bindings().providerInstances().isEmpty());
        assertEquals(0, InjectedConnectorProvider.unconfiguredStarts());
        assertEquals(0, InjectedConnectorProvider.configurationBindings("first"));
        CommandDescriptor firstOperation = descriptor("cdi-first", "inspect.first");
        PipelineExecutionContext execution = new PipelineExecutionContext("connector-tenant", "connector-execution", 0);
        String replayId = "connector-replay-only";
        CommandRequest<String> replayRequest = new CommandRequest<>(
            firstOperation, replayId, "ignored", execution, firstOperation.config());
        fixture.store().createPending(replayRequest, System.currentTimeMillis()).await().atMost(TIMEOUT);
        fixture.store().markSucceeded(
            execution.tenantId(), replayId, "recorded", System.currentTimeMillis()).await().atMost(TIMEOUT);

        PipelineExecutionContextHolder.set(execution);
        try {
            String replayed = fixture.commands().<String, String>execute(
                firstOperation, (descriptor, input) -> replayId, "ignored").await().atMost(TIMEOUT);
            assertEquals("recorded", replayed);
        } finally {
            PipelineExecutionContextHolder.clear();
        }
        assertTrue(fixture.bindings().providerInstances().isEmpty());
        assertEquals(0, InjectedConnectorProvider.configurationBindings("first"));
        fixture.bindings().stop(runtimeContext).toCompletableFuture().join();
    }

    @Test
    void bindingsOwnDistinctInjectedInstancesWhileOperationsShareTheirBinding() {
        Fixture fixture = fixture("cdi-first", "cdi-second");
        PipelineExecutionContext execution = new PipelineExecutionContext("connector-tenant", "connector-execution", 0);

        InjectedConnectorProvider.InvocationResult first = invoke(
            fixture.commands(), execution, "cdi-first", "inspect.first", "live-first", "one");
        assertEquals("injected", first.injection());
        assertEquals("first", first.binding());
        assertEquals(1, fixture.bindings().providerInstances().size());
        assertEquals(1, InjectedConnectorProvider.configurationBindings("first"));
        assertEquals(1, InjectedConnectorProvider.starts(first.providerInstance()));

        InjectedConnectorProvider.InvocationResult shared = invoke(
            fixture.commands(), execution, "cdi-first", "inspect.second", "live-shared", "two");
        assertEquals(first.providerInstance(), shared.providerInstance());
        assertEquals(1, InjectedConnectorProvider.configurationBindings("first"));
        assertEquals(1, InjectedConnectorProvider.starts(first.providerInstance()));

        InjectedConnectorProvider.InvocationResult second = invoke(
            fixture.commands(), execution, "cdi-second", "inspect.first", "live-second", "three");
        assertNotEquals(first.providerInstance(), second.providerInstance());
        assertEquals(2, fixture.bindings().providerInstances().size());
        assertEquals(1, InjectedConnectorProvider.configurationBindings("second"));
        assertEquals(1, InjectedConnectorProvider.starts(second.providerInstance()));
        assertEquals(0, InjectedConnectorProvider.unconfiguredStarts());
        fixture.bindings().stop(runtimeContext).toCompletableFuture().join();
        assertEquals(1, InjectedConnectorProvider.stops(first.providerInstance()));
        assertEquals(1, InjectedConnectorProvider.stops(second.providerInstance()));
    }

    @Test
    void shutdownWinsTheActivationRaceAndRemainsIdempotent() {
        Fixture fixture = fixture("cdi-racing");

        CompletionStage<Void> racing = fixture.bindings().activate(
            ConnectorBindingName.of("cdi-racing"), runtimeContext);
        CompletionStage<Void> stopped = fixture.bindings().stop(runtimeContext);
        assertFalse(stopped.toCompletableFuture().isDone());
        RuntimeException rejected = assertThrows(RuntimeException.class, () -> fixture.bindings().activate(
            ConnectorBindingName.of("cdi-racing"), runtimeContext).toCompletableFuture().join());
        assertTrue(rejected.getCause().getMessage().contains("shutdown has begun"), rejected.getMessage());

        InjectedConnectorProvider.releaseRacingStart();
        racing.toCompletableFuture().join();
        stopped.toCompletableFuture().join();
        int racingInstance = InjectedConnectorProvider.instanceFor("racing");
        assertEquals(1, InjectedConnectorProvider.configurationBindings("racing"));
        assertEquals(1, InjectedConnectorProvider.starts(racingInstance));
        assertEquals(1, InjectedConnectorProvider.stops(racingInstance));
        assertSame(stopped, fixture.bindings().stop(runtimeContext));
        assertEquals(1, InjectedConnectorProvider.stops(racingInstance));
        assertEquals(0, InjectedConnectorProvider.unconfiguredStarts());
    }

    private Fixture fixture(String... names) {
        List<ConnectorProvider<?>> providers = providerInstances.stream()
            .filter(provider -> ConnectorProviderId.of("test.cdi").equals(provider.id()))
            .toList();
        List<ConnectorBindingDefinition> definitions = java.util.Arrays.stream(names)
            .map(name -> new ConnectorBindingDefinition(
                ConnectorBindingName.of(name),
                ConnectorProviderId.of("test.cdi"),
                1,
                new ConnectorConfigurationDocument(Map.of("name", name.substring("cdi-".length())))))
            .toList();
        ConnectorBindingRegistry bindings = ConnectorBindingRegistry.fromProviders(
            definitions, providers, providerInstanceFactory);
        InMemoryCommandEffectStore store = new InMemoryCommandEffectStore();
        CommandStepSupport commands = new CommandStepSupport(
            new ConnectorRegistry(providers), bindings, List.of(store), queueAsyncConfig());
        return new Fixture(bindings, store, commands);
    }

    private record Fixture(
        ConnectorBindingRegistry bindings,
        InMemoryCommandEffectStore store,
        CommandStepSupport commands
    ) {
    }

    private static InjectedConnectorProvider.InvocationResult invoke(
        CommandStepSupport commands,
        PipelineExecutionContext execution,
        String binding,
        String operation,
        String commandId,
        String suffix
    ) {
        PipelineExecutionContextHolder.set(execution);
        try {
            return commands.<String, InjectedConnectorProvider.InvocationResult>execute(
                descriptor(binding, operation), (descriptor, input) -> commandId, "input")
                .await().atMost(TIMEOUT);
        } finally {
            PipelineExecutionContextHolder.clear();
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
