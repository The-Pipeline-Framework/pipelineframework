package org.pipelineframework.awaitable;

import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitInteractionStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitUnitStore;
import org.pipelineframework.command.*;
import org.pipelineframework.connector.*;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.orchestrator.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class CommandDeferredCompletionSupportTest {
    private final InMemoryAwaitInteractionStore interactions = new InMemoryAwaitInteractionStore();
    private final InMemoryAwaitUnitStore units = new InMemoryAwaitUnitStore();
    private final InMemoryCommandEffectStore effects = new InMemoryCommandEffectStore();
    private final AwaitResumeTokenService tokens = new AwaitResumeTokenService("test-secret");
    private final PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);

    @AfterEach
    void clearContext() {
        PipelineExecutionContextHolder.clear();
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({"false,true", "true,true", "false,false", "true,false"})
    void callbackAndDurableEffectJoinInEitherOrderWithoutRedispatch(boolean ambiguous, boolean callbackFirst) throws Exception {
        when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        var bindings = ConnectorBindingRegistry.fromProviders(List.of(new ConnectorBindingDefinition(
            ConnectorBindingName.of("jobs"), ConnectorProviderId.of("test.jobs"), 1, ConnectorConfigurationDocument.empty())),
            List.of(new JobProvider()));
        bindings.start(ConnectorRuntimeContext.empty()).toCompletableFuture().get(5, TimeUnit.SECONDS);
        var provider = (JobProvider) bindings.providerInstances().getFirst();
        var commands = new CommandStepSupport(new ConnectorRegistry(List.of()), bindings, List.of(effects), config);
        var identity = new ConnectorOperationIdentity(ConnectorProviderId.of("test.jobs"), "start", ConnectorOperationKind.COMMAND, 1);
        var command = CommandDescriptor.nativeCommand("StartJob", new NativeCommandSelector(
            Optional.of(ConnectorBindingName.of("jobs")), identity, 1, CommandPolicy.none()), String.class.getName(),
            String.class.getName(), "test-id", CommandDuplicatePolicy.RETURN_RECORDED, Map.of());
        var selection = new ConnectorCallbackSelection(ConnectorBindingName.of("jobs"), identity, 1,
            provider.operation.callbacks().getFirst(), "test.EndpointResolver", "test.Authenticator");
        var descriptor = new AwaitCompletionDescriptor("StartJob", String.class.getName(), String.class.getName(), "ONE_TO_ONE",
            Duration.ofMinutes(1), "signedResumeToken", "", Map.of(), List.of(), String.class.getName(), String.class.getName(),
            Function.identity(), Function.identity(), "test.Projector", (input, completion, metadata) -> input + ":" + completion,
            true, Optional.of(selection));
        AwaitCoordinator coordinator = coordinator();
        CommandDeferredCompletionSupport support = support(coordinator);
        AtomicReference<AwaitInteractionRecord> registered = new AtomicReference<>();
        AtomicReference<String> token = new AtomicReference<>();
        org.pipelineframework.connector.ProviderCallbackEndpointResolver endpoint = record -> {
            assertEquals(AwaitInteractionStatus.WAITING, interactions.get(record.tenantId(), record.interactionId())
                .await().indefinitely().orElseThrow().status());
            assertTrue(effects.find("tenant", "effect").await().indefinitely().isEmpty());
            registered.set(interactions.get(record.tenantId(), record.interactionId()).await().indefinitely().orElseThrow());
            return URI.create("https://app.test/");
        };
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 0));
        CompletableFuture<String> result = support.<String, String>execute(descriptor, "input", endpoint,
            callback -> commands.<String, String>execute(command, (ignored, input) -> "effect", "input", callback))
            .subscribeAsCompletionStage().toCompletableFuture();
        assertTrue(provider.operation.entered.await(5, TimeUnit.SECONDS));
        var record = registered.get();
        var callbackUri = provider.operation.callback.orElseThrow().callbackUri();
        token.set(callbackUri.getPath().substring(("/" + CommandDeferredCompletionSupport.CALLBACK_PATH).length()));
        assertEquals("https://app.test/pipeline/callbacks/" + token.get(), callbackUri.toString());
        assertEquals(CommandEffectStatus.DISPATCHING, effects.find("tenant", "effect").await().indefinitely().orElseThrow().status());
        assertThrows(AwaitSuspendedException.class, () -> support(coordinator()).execute(descriptor, "input",
            ignored -> URI.create("https://app.test/"),
            callback -> commands.execute(command, (ignored, input) -> "effect", "input", callback))
            .await().atMost(Duration.ofSeconds(5)));
        assertEquals(AwaitInteractionStatus.DISPATCHING,
            interactions.get("tenant", record.interactionId()).await().indefinitely().orElseThrow().status());
        assertEquals(1, provider.operation.dispatches.get());
        CommandOutcome<String> outcome = ambiguous ? new CommandOutcome.Ambiguous<>("unknown", List.of())
            : new CommandOutcome.Succeeded<>("accepted", CommandConfirmation.none(), List.of());
        if (!callbackFirst) {
            provider.operation.response.complete(outcome);
            var suspended = assertThrows(java.util.concurrent.ExecutionException.class,
                () -> result.get(5, TimeUnit.SECONDS));
            assertInstanceOf(AwaitSuspendedException.class, suspended.getCause());
            assertEquals(AwaitInteractionStatus.DISPATCHED,
                interactions.get("tenant", record.interactionId()).await().indefinitely().orElseThrow().status());
            assertThrows(AwaitSuspendedException.class, () -> support(coordinator()).execute(descriptor, "input",
                ignored -> { throw new AssertionError("pending recovery must reuse callback authority"); },
                ignored -> { throw new AssertionError("settled effect must not dispatch again"); }).await().indefinitely());
        }
        var observed = coordinator.complete(new AwaitCompletionCommand("tenant", record.interactionId(), record.correlationId(),
            token.get(), "completion", "done", "provider", System.currentTimeMillis())).await().indefinitely();
        if (!callbackFirst) {
            assertEquals(AwaitInteractionStatus.COMPLETED, observed.record().status());
            assertEquals("input:done", coordinator.resumePayload(observed.record()));
            assertEquals(ambiguous ? CommandEffectStatus.AMBIGUOUS : CommandEffectStatus.SUCCEEDED,
                effects.find("tenant", "effect").await().indefinitely().orElseThrow().status());
            // Ordinary Await admission owns late-callback continuation. Redelivery of
            // the original Command must not also return a result to its worker.
            assertThrows(AwaitSuspendedException.class, () -> support(coordinator()).execute(descriptor, "input",
                ignored -> { throw new AssertionError("completed recovery must reuse callback authority"); },
                ignored -> { throw new AssertionError("completed effect must not dispatch again"); })
                .await().atMost(Duration.ofSeconds(5)));
            assertEquals(1, provider.operation.dispatches.get());
            return;
        }
        assertEquals(AwaitInteractionStatus.COMPLETION_OBSERVED, observed.record().status());
        assertFalse(result.isDone());
        assertNotEquals(AwaitUnitStatus.COMPLETED, units.get("tenant", record.unitId()).await().indefinitely().orElseThrow().status());
        provider.operation.response.complete(outcome);
        assertEquals("input:done", result.get(5, TimeUnit.SECONDS));
        var effect = effects.find("tenant", "effect").await().indefinitely().orElseThrow();
        assertEquals(ambiguous ? CommandEffectStatus.AMBIGUOUS : CommandEffectStatus.SUCCEEDED, effect.status());
        assertFalse(effect.toString().contains(token.get()));
        var restarted = support(coordinator());
        assertEquals("input:done", restarted.<String, String>execute(descriptor, "input",
            ignored -> { throw new AssertionError("completed interaction must not resolve a fresh URI"); },
            ignored -> { throw new AssertionError("completed interaction must not invoke Command again"); })
            .await().atMost(Duration.ofSeconds(5)));
    }

    private AwaitCoordinator coordinator() {
        var coordinator = new AwaitCoordinator();
        coordinator.interactionStores = instances(interactions);
        coordinator.unitStores = instances(units);
        coordinator.orchestratorConfig = config;
        coordinator.resumeTokenService = tokens;
        coordinator.descriptorFactory = new AwaitCompletionDescriptorRegistry();
        return coordinator;
    }

    private CommandDeferredCompletionSupport support(AwaitCoordinator coordinator) {
        var support = new CommandDeferredCompletionSupport();
        support.coordinator = coordinator;
        support.tokens = tokens;
        support.config = config;
        return support;
    }

    @SuppressWarnings("unchecked")
    private <T> Instance<T> instances(T value) {
        Instance<T> instance = mock(Instance.class);
        when(instance.stream()).thenAnswer(ignored -> java.util.stream.Stream.of(value));
        return instance;
    }

    public record JobConfiguration() { }

    public static final class JobProvider implements ConnectorProvider<Void> {
        final JobOperation operation = new JobOperation();
        @Override public ConnectorProviderId id() { return ConnectorProviderId.of("test.jobs"); }
        @Override public ConnectorProviderVersion version() { return new ConnectorProviderVersion(1, 0); }
        @Override public Collection<? extends ConnectorOperation> operations() { return List.of(operation); }
    }

    public static final class JobOperation implements CommandOperation<String, JobConfiguration, String> {
        final CompletableFuture<CommandOutcome<String>> response = new CompletableFuture<>();
        final CountDownLatch entered = new CountDownLatch(1);
        final java.util.concurrent.atomic.AtomicInteger dispatches = new java.util.concurrent.atomic.AtomicInteger();
        Optional<ConnectorCallbackContext> callback = Optional.empty();
        @Override public String id() { return "start"; }
        @Override public Optional<ConnectorConfigSchema<JobConfiguration>> configurationSchema() {
            return Optional.of(ConnectorConfigSchema.record(JobConfiguration.class, "test.jobs.start", 1));
        }
        @Override public List<ConnectorOperationCallbackDescriptor> callbacks() {
            return List.of(new ConnectorOperationCallbackDescriptor("completed",
                new ConnectorOperationTypeContract(String.class.getName(), Optional.empty()), true));
        }
        @Override public CompletionStage<CommandOutcome<String>> dispatch(CommandInvocation<String, JobConfiguration> invocation) {
            dispatches.incrementAndGet();
            callback = invocation.callbackContext();
            entered.countDown();
            return response;
        }
    }
}
