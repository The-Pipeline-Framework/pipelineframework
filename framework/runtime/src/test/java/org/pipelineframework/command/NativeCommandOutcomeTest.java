package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.connector.CommandCapabilities;
import org.pipelineframework.connector.CommandConfirmation;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandOperation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.CommandPolicy;
import org.pipelineframework.connector.CommandReference;
import org.pipelineframework.connector.CommandReferencePurpose;
import org.pipelineframework.connector.ConnectorCompletionStages;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorConfigurationSnapshot;
import org.pipelineframework.connector.ConnectorExecutionCapabilities;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.connector.ConnectorExecutionStyle;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorOperationDescriptor;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderDescriptor;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRegistry;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.ConnectorConcurrencyScope;

class NativeCommandOutcomeTest {
    private final InMemoryCommandEffectStore store = new InMemoryCommandEffectStore();
    private final NativeOperation operation = new NativeOperation();
    private final CommandStepSupport support = new CommandStepSupport(
        new ConnectorRegistry(List.of(new NativeProvider(operation))),
        List.of(store),
        queueAsyncConfig());

    @AfterEach
    void clearContext() {
        AwaitExecutionContextHolder.clear();
    }

    @Test
    void recordsSuccessWithOnlyDeclaredSafeReferencesAndReplaysWithoutResolvingProvider() {
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "execution", 1));
        operation.outcome = new CommandOutcome.Succeeded<>(
            "done",
            new CommandConfirmation(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, false),
            Set.of("created"),
            List.of(
                new CommandReference("ticket", "TKT-1", CommandReferencePurpose.RECONCILIATION),
                new CommandReference("secret", "do-not-persist", CommandReferencePurpose.CORRELATION)));

        assertEquals("done", support.<String, String>execute(descriptor(), (ignored, input) -> "stable-1", "input")
            .await().atMost(Duration.ofSeconds(5)));
        CommandEffectRecord record = store.find("tenant", "stable-1").await().atMost(Duration.ofSeconds(5)).orElseThrow();
        assertEquals(CommandEffectStatus.SUCCEEDED, record.status());
        assertEquals(List.of(new CommandReference("ticket", "TKT-1", CommandReferencePurpose.RECONCILIATION)),
            record.outcome().orElseThrow().references());
        assertEquals(Set.of("created"), record.outcome().orElseThrow().flags());
        assertFalse(record.outcome().orElseThrow().toString().contains("do-not-persist"));

        operation.failIfInvoked = true;
        assertEquals("done", support.<String, String>execute(descriptor(), (ignored, input) -> "stable-1", "input")
            .await().atMost(Duration.ofSeconds(5)));
        assertEquals(1, operation.invocations);
    }

    @Test
    void mapsEveryNonSuccessOutcomeToItsDurableEffectState() {
        assertOutcome(new CommandOutcome.RetryableFailure<>("temporarily-unavailable", List.of()),
            CommandEffectStatus.FAILED_RETRYABLE, CommandRetryableOutcomeException.class);
        assertOutcome(new CommandOutcome.TerminalFailure<>("invalid-request", List.of()),
            CommandEffectStatus.DLQ, CommandOutcomeException.class);
        assertOutcome(new CommandOutcome.Ambiguous<>("submission-unknown", List.of()),
            CommandEffectStatus.AMBIGUOUS, CommandOutcomeException.class);
        assertOutcome(new CommandOutcome.UserActionRequired<>("approve-in-browser", List.of()),
            CommandEffectStatus.USER_ACTION_REQUIRED, CommandOutcomeException.class);
    }

    @Test
    void rejectsInvalidConfigurationBeforeCreatingAnEffectOrInvokingTheProvider() {
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "execution", 1));

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> support.<String, String>execute(
            descriptor(Map.of("unknown", "value")), (ignored, input) -> "invalid-config", "input")
            .await().atMost(Duration.ofSeconds(5)));

        assertFalse(failure.getMessage().isBlank());
        assertEquals(0, operation.invocations);
        assertFalse(store.find("tenant", "invalid-config").await().atMost(Duration.ofSeconds(5)).isPresent());
    }

    @Test
    void rejectsAnUnknownNativeOperationBeforeCreatingAnEffect() {
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "execution", 1));
        InMemoryCommandEffectStore missingStore = new InMemoryCommandEffectStore();
        CommandStepSupport missingProviderSupport = new CommandStepSupport(
            new ConnectorRegistry(List.of()), List.of(missingStore), queueAsyncConfig());

        IllegalStateException failure = assertThrows(IllegalStateException.class, () -> missingProviderSupport.<String, String>execute(
            descriptor(), (ignored, input) -> "missing-operation", "input").await().atMost(Duration.ofSeconds(5)));

        assertEquals("no connector provider registered for ID: acme.search", failure.getMessage());
        assertFalse(missingStore.find("tenant", "missing-operation").await().atMost(Duration.ofSeconds(5)).isPresent());
    }

    @Test
    void rejectsAStoreWithoutNativeOutcomeSupportBeforeItCreatesAnEffect() {
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "execution", 1));
        CommandEffectStore legacyStore = mock(CommandEffectStore.class);
        when(legacyStore.find("tenant", "unsupported-store")).thenReturn(io.smallrye.mutiny.Uni.createFrom().item(Optional.empty()));
        CommandStepSupport legacyStoreSupport = new CommandStepSupport(
            new ConnectorRegistry(List.of(new NativeProvider(operation))), List.of(legacyStore), queueAsyncConfig());

        IllegalStateException failure = assertThrows(IllegalStateException.class, () -> legacyStoreSupport.<String, String>execute(
            descriptor(), (ignored, input) -> "unsupported-store", "input").await().atMost(Duration.ofSeconds(5)));

        assertTrue(failure.getMessage().contains("requires a CommandEffectStore that persists outcome snapshots"));
        verify(legacyStore).find("tenant", "unsupported-store");
        verify(legacyStore, never()).createPending(any(), anyLong());
    }

    @Test
    void keepsLegacyEffectRecordsFreeOfNativeOutcomeSnapshots() {
        CommandEffectRecord legacy = new CommandEffectRecord(
            "tenant", "execution", "step", "command", "legacy-id", CommandEffectStatus.PENDING,
            "input", "output", "", "", 1L, 1L);

        assertTrue(legacy.outcome().isEmpty());
    }

    @Test
    void excludesUndeclaredReferencesAndHumanInstructionsFromDurableOutcomeJson() throws Exception {
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "execution", 1));
        operation.outcome = new CommandOutcome.UserActionRequired<>(
            "approval-required",
            "operator-only-secret-instruction",
            List.of(new CommandReference("secret", "operator-only-secret-reference", CommandReferencePurpose.CORRELATION)));

        CommandOutcomeException failure = assertThrows(CommandOutcomeException.class, () -> support.<String, String>execute(
            descriptor(), (ignored, input) -> "user-action", "input").await().atMost(Duration.ofSeconds(5)));
        CommandEffectRecord record = store.find("tenant", "user-action").await().atMost(Duration.ofSeconds(5)).orElseThrow();
        String json = org.pipelineframework.config.pipeline.PipelineJson.mapper().writeValueAsString(record);

        assertEquals(CommandEffectStatus.USER_ACTION_REQUIRED, failure.status());
        assertFalse(json.contains("operator-only-secret-instruction"));
        assertFalse(json.contains("operator-only-secret-reference"));
        assertFalse(failure.getMessage().contains("operator-only-secret"));
    }

    @Test
    void rejectsInvalidTerminalOutcomeExceptionArguments() {
        assertThrows(NullPointerException.class, () -> new CommandOutcomeException(null, "failure"));
        assertThrows(IllegalArgumentException.class, () -> new CommandOutcomeException(CommandEffectStatus.DLQ, " "));
    }

    private void assertOutcome(CommandOutcome<String> outcome, CommandEffectStatus status, Class<? extends Throwable> type) {
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "execution", 1));
        operation.outcome = outcome;
        String commandId = "stable-" + status.name().toLowerCase();
        Throwable failure = assertThrows(type, () -> support.<String, String>execute(
            descriptor(), (ignored, input) -> commandId, "input").await().atMost(Duration.ofSeconds(5)));
        String expectedMessage = "command outcome " + status.name().toLowerCase() + ": " + outcome.code();
        assertEquals(expectedMessage, failure.getMessage());
        assertEquals(status, store.find("tenant", commandId).await().atMost(Duration.ofSeconds(5)).orElseThrow().status());
    }

    private static CommandDescriptor descriptor() {
        return descriptor(Map.of("target", "orders"));
    }

    private static CommandDescriptor descriptor(Map<String, Object> configuration) {
        NativeCommandSelector selector = new NativeCommandSelector(
            new ConnectorOperationIdentity(ConnectorProviderId.of("acme.search"), "write.document", ConnectorOperationKind.COMMAND, 1),
            1,
            CommandPolicy.none());
        return CommandDescriptor.nativeCommand(
            "NativeWriteService", selector, String.class.getName(), String.class.getName(), "test", CommandDuplicatePolicy.RETURN_RECORDED,
            configuration);
    }

    private static org.pipelineframework.orchestrator.PipelineOrchestratorConfig queueAsyncConfig() {
        org.pipelineframework.orchestrator.PipelineOrchestratorConfig config = mock(org.pipelineframework.orchestrator.PipelineOrchestratorConfig.class);
        when(config.mode()).thenReturn(org.pipelineframework.orchestrator.OrchestratorMode.QUEUE_ASYNC);
        return config;
    }

    public record OperationConfig(String target) {
    }

    private static final class NativeProvider implements ConnectorProvider<Void> {
        private final NativeOperation operation;

        private NativeProvider(NativeOperation operation) {
            this.operation = operation;
        }

        @Override
        public ConnectorProviderDescriptor descriptor() {
            return new ConnectorProviderDescriptor(
                ConnectorProviderId.of("acme.search"), new ConnectorProviderVersion(1, 0), Optional.empty(),
                Optional.of(new ConnectorExecutionCapabilities(
                    ConnectorExecutionStyle.PROVIDER_MANAGED, ConnectorConcurrencyScope.PROVIDER_MANAGED)));
        }

        @Override
        public Collection<? extends ConnectorOperation> operations() {
            return List.of(operation);
        }

        @Override
        public org.pipelineframework.connector.ConnectorExecutionCapabilities executionCapabilities() {
            return descriptor().executionCapabilities().orElseThrow();
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

    private static final class NativeOperation implements CommandOperation<String, OperationConfig, String> {
        private CommandOutcome<String> outcome;
        private int invocations;
        private boolean failIfInvoked;

        @Override
        public ConnectorOperationDescriptor descriptor() {
            return new ConnectorOperationDescriptor(
                "write.document", ConnectorOperationKind.COMMAND, 1,
                Optional.of(schema().descriptor()),
                Optional.of(declaredCapabilities()));
        }

        @Override
        public Optional<ConnectorConfigSchema<OperationConfig>> configurationSchema() {
            return Optional.of(schema());
        }

        @Override
        public CommandCapabilities capabilities() {
            return declaredCapabilities();
        }

        @Override
        public CompletionStage<CommandOutcome<String>> dispatch(
            org.pipelineframework.connector.CommandInvocation<String, OperationConfig> invocation
        ) {
            if (failIfInvoked) {
                return CompletableFuture.failedFuture(new AssertionError("provider should not be invoked during successful replay"));
            }
            invocations++;
            return CompletableFuture.completedFuture(outcome);
        }

        private static ConnectorConfigSchema<OperationConfig> schema() {
            return ConnectorConfigSchema.record(OperationConfig.class, "acme.search.write.document", 1);
        }

        private static CommandCapabilities declaredCapabilities() {
            return new CommandCapabilities(false, true, true, CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, false, Set.of("ticket"));
        }
    }
}
