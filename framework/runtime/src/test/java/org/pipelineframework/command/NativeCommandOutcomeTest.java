package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.connector.CommandCapabilities;
import org.pipelineframework.connector.CommandConfirmation;
import org.pipelineframework.connector.CommandDispatchIdentity;
import org.pipelineframework.connector.CommandExecutionPosture;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandOperation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.CommandPolicy;
import org.pipelineframework.connector.CommandReference;
import org.pipelineframework.connector.CommandReferencePurpose;
import org.pipelineframework.connector.ConnectorBindingDefinition;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorCompletionStages;
import org.pipelineframework.connector.ConnectorConfigurationDocument;
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

    @Test
    void bindingQualifiedCommandNamesSeparateSharedProviderOperations() {
        ConnectorOperationIdentity identity = new ConnectorOperationIdentity(
            ConnectorProviderId.of("acme.search"), "write.document", ConnectorOperationKind.COMMAND, 1);
        NativeCommandSelector first = new NativeCommandSelector(
            Optional.of(ConnectorBindingName.of("first")), identity, 1, CommandPolicy.none());
        NativeCommandSelector second = new NativeCommandSelector(
            Optional.of(ConnectorBindingName.of("second")), identity, 1, CommandPolicy.none());

        assertEquals("native-binding:first/write.document", first.commandName());
        assertEquals("native-binding:second/write.document", second.commandName());
    }

    @AfterEach
    void clearContext() {
        PipelineExecutionContextHolder.clear();
    }

    @Test
    void recordsSuccessWithOnlyDeclaredSafeReferencesAndReplaysWithoutResolvingProvider() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 1));
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
        assertEquals(1, record.outcome().orElseThrow().providerMajorVersion());
        assertFalse(record.outcome().orElseThrow().toString().contains("do-not-persist"));

        CommandStepSupport replayWithoutProvider = new CommandStepSupport(
            new ConnectorRegistry(List.of()), List.of(store), queueAsyncConfig());
        NativeCommandSelector changedSelector = new NativeCommandSelector(
            new ConnectorOperationIdentity(
                ConnectorProviderId.of("other.provider"), "different.operation", ConnectorOperationKind.COMMAND, 9),
            7,
            CommandPolicy.none());
        CommandDescriptor changedDescriptor = CommandDescriptor.nativeCommand(
            "DifferentNativeService", changedSelector, Integer.class.getName(), String.class.getName(), "test",
            CommandDuplicatePolicy.RETURN_RECORDED, Map.of("different", "configuration"));

        assertEquals("done", replayWithoutProvider.<String, String>execute(
            changedDescriptor, (ignored, input) -> "stable-1", "input")
            .await().atMost(Duration.ofSeconds(5)));
        assertEquals(1, operation.invocations);
    }

    @Test
    void routesThroughNamedBindingAndActivatesItOnlyForLiveDispatch() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 1));
        NativeProvider prototype = new NativeProvider();
        ConnectorBindingRegistry bindings = ConnectorBindingRegistry.fromProviders(
            List.of(new ConnectorBindingDefinition(
                ConnectorBindingName.of("work"),
                ConnectorProviderId.of("acme.search"),
                1,
                ConnectorConfigurationDocument.empty())),
            List.of(prototype));
        assertTrue(bindings.providerInstances().isEmpty());
        CommandStepSupport boundSupport = new CommandStepSupport(
            new ConnectorRegistry(List.of()), bindings, List.of(store), queueAsyncConfig());
        NativeCommandSelector selector = new NativeCommandSelector(
            Optional.of(ConnectorBindingName.of("work")),
            new ConnectorOperationIdentity(
                ConnectorProviderId.of("acme.search"), "write.document", ConnectorOperationKind.COMMAND, 1),
            1,
            CommandPolicy.none());
        CommandDescriptor descriptor = CommandDescriptor.nativeCommand(
            "BoundNativeService", selector, String.class.getName(), String.class.getName(), "test",
            CommandDuplicatePolicy.RETURN_RECORDED, Map.of("target", "orders"));

        assertEquals("bound-result", boundSupport.<String, String>execute(
            descriptor, (ignored, input) -> "bound-command", "input").await().atMost(Duration.ofSeconds(5)));
        assertEquals(1, bindings.providerInstances().size());
        NativeProvider boundProvider = (NativeProvider) bindings.providerInstances().getFirst();
        assertEquals(1, boundProvider.starts);
        assertEquals(1, boundProvider.operation.invocations);
        assertEquals(0, prototype.starts);
        assertEquals(0, prototype.operation.invocations);
    }

    @Test
    void rejectsBindingWhoseRuntimeProviderDoesNotMatchTheSelector() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 1));
        ConnectorBindingRegistry bindings = ConnectorBindingRegistry.fromProviders(
            List.of(new ConnectorBindingDefinition(
                ConnectorBindingName.of("work"),
                ConnectorProviderId.of("acme.search"),
                1,
                ConnectorConfigurationDocument.empty())),
            List.of(new NativeProvider()));
        CommandStepSupport boundSupport = new CommandStepSupport(
            new ConnectorRegistry(List.of()), bindings, List.of(store), queueAsyncConfig());
        NativeCommandSelector mismatched = new NativeCommandSelector(
            Optional.of(ConnectorBindingName.of("work")),
            new ConnectorOperationIdentity(
                ConnectorProviderId.of("other.search"), "write.document", ConnectorOperationKind.COMMAND, 1),
            1,
            CommandPolicy.none());
        CommandDescriptor descriptor = CommandDescriptor.nativeCommand(
            "MismatchedBindingService", mismatched, String.class.getName(), String.class.getName(), "test",
            CommandDuplicatePolicy.RETURN_RECORDED, Map.of("target", "orders"));

        RuntimeException failure = assertThrows(RuntimeException.class, () -> boundSupport.<String, String>execute(
            descriptor, (ignored, input) -> "mismatched-binding", "input").await().atMost(Duration.ofSeconds(5)));

        assertTrue(failure.getMessage().contains("command descriptor requires other.search v1"), failure.getMessage());
    }

    @Test
    void mapsInsufficientAchievedConfirmationToNonRetryableBarriers() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 1));
        operation.outcome = new CommandOutcome.Succeeded<>(
            "done", CommandConfirmation.none(), Set.of(), List.of());
        CommandPolicy machinePolicy = new CommandPolicy(
            false, false, false, Optional.empty(), Optional.empty(),
            Optional.of(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED), false);

        CommandOutcomeException machineFailure = assertThrows(CommandOutcomeException.class, () -> support.<String, String>execute(
            descriptor(machinePolicy), (ignored, input) -> "insufficient-machine", "input")
            .await().atMost(Duration.ofSeconds(5)));
        CommandEffectRecord machineRecord = store.find("tenant", "insufficient-machine")
            .await().atMost(Duration.ofSeconds(5)).orElseThrow();
        assertEquals(CommandEffectStatus.AMBIGUOUS, machineFailure.status());
        assertEquals("machine-confirmation-insufficient", machineFailure.outcomeCode());
        assertEquals(CommandEffectStatus.AMBIGUOUS, machineRecord.status());
        assertEquals("machine-confirmation-insufficient", machineRecord.outcome().orElseThrow().outcomeCode());
        assertNull(machineRecord.output());

        operation.outcome = new CommandOutcome.Succeeded<>(
            "done",
            new CommandConfirmation(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, false),
            Set.of(), List.of());
        CommandPolicy userPolicy = new CommandPolicy(
            false, false, false, Optional.empty(), Optional.empty(), Optional.empty(), true);
        CommandOutcomeException userFailure = assertThrows(CommandOutcomeException.class, () -> support.<String, String>execute(
            descriptor(userPolicy), (ignored, input) -> "missing-user-confirmation", "input")
            .await().atMost(Duration.ofSeconds(5)));
        CommandEffectRecord userRecord = store.find("tenant", "missing-user-confirmation")
            .await().atMost(Duration.ofSeconds(5)).orElseThrow();
        assertEquals(CommandEffectStatus.USER_ACTION_REQUIRED, userFailure.status());
        assertEquals("user-confirmation-required", userFailure.outcomeCode());
        assertEquals(CommandEffectStatus.USER_ACTION_REQUIRED, userRecord.status());
        assertEquals("user-confirmation-required", userRecord.outcome().orElseThrow().outcomeCode());
    }

    @Test
    void recordsSuccessWhenTheAchievedConfirmationSatisfiesPolicy() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 1));
        operation.outcome = new CommandOutcome.Succeeded<>(
            "done",
            new CommandConfirmation(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, true),
            Set.of(), List.of());
        CommandPolicy policy = new CommandPolicy(
            false, false, false, Optional.empty(), Optional.empty(),
            Optional.of(CommandMachineConfirmation.SUBMITTED), true);

        assertEquals("done", support.<String, String>execute(
            descriptor(policy), (ignored, input) -> "confirmed-success", "input")
            .await().atMost(Duration.ofSeconds(5)));
        assertEquals(CommandEffectStatus.SUCCEEDED, store.find("tenant", "confirmed-success")
            .await().atMost(Duration.ofSeconds(5)).orElseThrow().status());
    }

    @Test
    void mapsEveryNonSuccessOutcomeToItsDurableEffectState() {
        assertOutcome(new CommandOutcome.RetryableFailure<>("temporarily-unavailable", List.of()),
            CommandEffectStatus.FAILED_RETRYABLE, CommandRetryableOutcomeException.class);
        assertOutcome(new CommandOutcome.TerminalFailure<>("invalid-request", List.of()),
            CommandEffectStatus.DLQ, CommandOutcomeException.class);
        assertOutcome(new CommandOutcome.Ambiguous<>("submission-unknown", List.of()),
            CommandEffectStatus.AMBIGUOUS, CommandOutcomeException.class);
        assertOutcome(new CommandOutcome.UserActionRequired<>("approve-in-browser", "Approve the submission in the provider console", List.of()),
            CommandEffectStatus.USER_ACTION_REQUIRED, CommandOutcomeException.class);
    }

    @Test
    void deliberateNativeRetryKeepsLogicalIdentityAndChangesAttemptIdentity() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution-1", 1));
        operation.outcome = new CommandOutcome.RetryableFailure<>("temporarily-unavailable", List.of());
        assertThrows(CommandRetryableOutcomeException.class, () -> support.<String, String>execute(
            descriptor(), (ignored, input) -> "stable-retry", "input")
            .await().atMost(Duration.ofSeconds(5)));

        operation.outcome = new CommandOutcome.Succeeded<>(
            "done", CommandConfirmation.none(), Set.of(), List.of());
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution-2", 1));
        assertEquals("done", support.<String, String>retry(
            descriptor(), (ignored, input) -> "stable-retry", "input")
            .await().atMost(Duration.ofSeconds(5)));

        assertEquals(2, operation.invocations);
        assertEquals(2, operation.dispatchIdentities.size());
        assertEquals("stable-retry", operation.dispatchIdentities.get(0).commandId());
        assertEquals("stable-retry", operation.dispatchIdentities.get(1).commandId());
        assertFalse(operation.dispatchIdentities.get(0).attemptId()
            .equals(operation.dispatchIdentities.get(1).attemptId()));
        CommandEffectRecord record = store.find("tenant", "stable-retry")
            .await().atMost(Duration.ofSeconds(5)).orElseThrow();
        assertEquals(List.of(CommandEffectStatus.FAILED_RETRYABLE, CommandEffectStatus.SUCCEEDED),
            record.attempts().stream().map(CommandEffectAttemptRecord::status).toList());
    }

    @Test
    void deliberateNativeRetryRequiresProviderCapabilityBeforeCreatingAnAttempt() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution-1", 1));
        operation.retryRedriveSupported = false;
        operation.outcome = new CommandOutcome.RetryableFailure<>("temporarily-unavailable", List.of());
        assertThrows(CommandRetryableOutcomeException.class, () -> support.<String, String>execute(
            descriptor(), (ignored, input) -> "unsupported-retry", "input")
            .await().atMost(Duration.ofSeconds(5)));

        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution-2", 1));
        IllegalStateException rejected = assertThrows(IllegalStateException.class,
            () -> support.<String, String>retry(
                descriptor(), (ignored, input) -> "unsupported-retry", "input")
                .await().atMost(Duration.ofSeconds(5)));

        assertTrue(rejected.getMessage().contains("does not support deliberate retry/redrive"));
        assertEquals(1, operation.invocations);
        assertEquals(1, store.find("tenant", "unsupported-retry")
            .await().atMost(Duration.ofSeconds(5)).orElseThrow().attempts().size());
    }

    @Test
    void deliberateRetryDoesNotCrossAmbiguousOutcomeBarrier() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 1));
        operation.outcome = new CommandOutcome.Ambiguous<>("submission-unknown", List.of());
        assertThrows(CommandOutcomeException.class, () -> support.<String, String>execute(
            descriptor(), (ignored, input) -> "ambiguous-retry", "input")
            .await().atMost(Duration.ofSeconds(5)));

        CommandOutcomeException rejected = assertThrows(CommandOutcomeException.class,
            () -> support.<String, String>retry(
                descriptor(), (ignored, input) -> "ambiguous-retry", "input")
                .await().atMost(Duration.ofSeconds(5)));

        assertEquals(CommandEffectStatus.AMBIGUOUS, rejected.status());
        assertEquals(1, operation.invocations);
        assertEquals(1, store.find("tenant", "ambiguous-retry")
            .await().atMost(Duration.ofSeconds(5)).orElseThrow().attempts().size());
    }

    @Test
    void rejectsInvalidConfigurationBeforeCreatingAnEffectOrInvokingTheProvider() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 1));

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> support.<String, String>execute(
            descriptor(Map.of("unknown", "value")), (ignored, input) -> "invalid-config", "input")
            .await().atMost(Duration.ofSeconds(5)));

        assertFalse(failure.getMessage().isBlank());
        assertEquals(0, operation.invocations);
        assertFalse(store.find("tenant", "invalid-config").await().atMost(Duration.ofSeconds(5)).isPresent());
    }

    @Test
    void rejectsAnUnknownNativeOperationBeforeCreatingAnEffect() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 1));
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
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 1));
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
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 1));
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
        assertThrows(IllegalArgumentException.class, () -> new CommandRetryableOutcomeException(" "));
    }

    @Test
    void rejectsBlockingNativeProvidersBeforeCreatingAnEffect() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 1));
        CommandStepSupport blockingSupport = new CommandStepSupport(
            new ConnectorRegistry(List.of(new NativeProvider(operation, new ConnectorExecutionCapabilities(
                ConnectorExecutionStyle.BLOCKING, ConnectorConcurrencyScope.PROVIDER_MANAGED)))),
            List.of(store), queueAsyncConfig());

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> blockingSupport.<String, String>execute(
            descriptor(), (ignored, input) -> "blocking", "input").await().atMost(Duration.ofSeconds(5)));

        assertTrue(failure.getMessage().contains("declares blocking execution"));
        assertFalse(store.find("tenant", "blocking").await().atMost(Duration.ofSeconds(5)).isPresent());
        assertEquals(0, operation.invocations);
    }

    @Test
    void normalizesProviderThrowsWrappedFailuresAndNullResults() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 1));
        IllegalArgumentException immediate = new IllegalArgumentException("immediate-provider-bug");
        operation.immediateFailure = immediate;
        IllegalArgumentException observedImmediate = assertThrows(IllegalArgumentException.class, () -> support.<String, String>execute(
            descriptor(), (ignored, input) -> "immediate-provider-bug", "input").await().atMost(Duration.ofSeconds(5)));
        assertSame(immediate, observedImmediate);
        assertEquals(IllegalArgumentException.class.getName(), store.find("tenant", "immediate-provider-bug")
            .await().atMost(Duration.ofSeconds(5)).orElseThrow().errorClass());

        operation.immediateFailure = null;
        IllegalStateException wrapped = new IllegalStateException("wrapped-provider-bug");
        operation.stageOverride = CompletableFuture.failedFuture(new CompletionException(wrapped));
        IllegalStateException observedWrapped = assertThrows(IllegalStateException.class, () -> support.<String, String>execute(
            descriptor(), (ignored, input) -> "wrapped-provider-bug", "input").await().atMost(Duration.ofSeconds(5)));
        assertSame(wrapped, observedWrapped);
        assertEquals(IllegalStateException.class.getName(), store.find("tenant", "wrapped-provider-bug")
            .await().atMost(Duration.ofSeconds(5)).orElseThrow().errorClass());

        operation.stageOverride = null;
        operation.returnNullStage = true;
        IllegalStateException nullStage = assertThrows(IllegalStateException.class, () -> support.<String, String>execute(
            descriptor(), (ignored, input) -> "null-stage", "input").await().atMost(Duration.ofSeconds(5)));
        assertTrue(nullStage.getMessage().contains("returned a null CompletionStage"));
        assertEquals(CommandEffectStatus.FAILED_RETRYABLE, store.find("tenant", "null-stage")
            .await().atMost(Duration.ofSeconds(5)).orElseThrow().status());

        operation.returnNullStage = false;
        operation.outcome = null;
        IllegalStateException nullOutcome = assertThrows(IllegalStateException.class, () -> support.<String, String>execute(
            descriptor(), (ignored, input) -> "null-outcome", "input").await().atMost(Duration.ofSeconds(5)));
        assertTrue(nullOutcome.getMessage().contains("returned a null outcome"));
        assertEquals(CommandEffectStatus.FAILED_RETRYABLE, store.find("tenant", "null-outcome")
            .await().atMost(Duration.ofSeconds(5)).orElseThrow().status());
    }

    @Test
    void mapsCancelledProviderStagesToAnAmbiguousBarrier() {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 1));
        CompletableFuture<CommandOutcome<String>> cancelled = new CompletableFuture<>();
        cancelled.cancel(false);
        operation.stageOverride = cancelled;

        CommandOutcomeException failure = assertThrows(CommandOutcomeException.class, () -> support.<String, String>execute(
            descriptor(), (ignored, input) -> "cancelled-provider-stage", "input").await().atMost(Duration.ofSeconds(5)));
        CommandEffectRecord record = store.find("tenant", "cancelled-provider-stage")
            .await().atMost(Duration.ofSeconds(5)).orElseThrow();

        assertEquals(CommandEffectStatus.AMBIGUOUS, failure.status());
        assertEquals("provider-dispatch-cancelled", failure.outcomeCode());
        assertEquals(CommandEffectStatus.AMBIGUOUS, record.status());
        assertEquals(1, operation.invocations);
    }

    @Test
    void serializesProviderVersionAndReadsLegacyRecordsWithoutNativeSnapshots() throws Exception {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 1));
        operation.outcome = new CommandOutcome.Succeeded<>(
            "done", CommandConfirmation.none(), Set.of(), List.of());
        support.<String, String>execute(descriptor(), (ignored, input) -> "serialized-native", "input")
            .await().atMost(Duration.ofSeconds(5));

        CommandEffectRecord nativeRecord = store.find("tenant", "serialized-native")
            .await().atMost(Duration.ofSeconds(5)).orElseThrow();
        String nativeJson = org.pipelineframework.config.pipeline.PipelineJson.mapper().writeValueAsString(nativeRecord);
        assertTrue(nativeJson.contains("\"providerMajorVersion\":1"));
        CommandEffectRecord nativeRoundTrip = org.pipelineframework.config.pipeline.PipelineJson.mapper()
            .readValue(nativeJson, CommandEffectRecord.class);
        assertEquals(1, nativeRoundTrip.outcome().orElseThrow().providerMajorVersion());

        String legacyJson = """
            {"tenantId":"tenant","executionId":"execution","stepId":"step","command":"legacy.command",
            "commandId":"legacy-id","status":"SUCCEEDED","input":"in","output":"out",
            "errorClass":null,"errorMessage":null,"createdAtEpochMs":1,"updatedAtEpochMs":2}
            """;
        CommandEffectRecord legacy = org.pipelineframework.config.pipeline.PipelineJson.mapper()
            .readValue(legacyJson, CommandEffectRecord.class);
        assertTrue(legacy.outcome().isEmpty());
        assertEquals("out", legacy.output());
    }

    private void assertOutcome(CommandOutcome<String> outcome, CommandEffectStatus status, Class<? extends Throwable> type) {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant", "execution", 1));
        operation.outcome = outcome;
        String commandId = "stable-" + status.name().toLowerCase(Locale.ROOT);
        int invocationsBefore = operation.invocations;
        Throwable failure = assertThrows(type, () -> support.<String, String>execute(
            descriptor(), (ignored, input) -> commandId, "input").await().atMost(Duration.ofSeconds(5)));
        String expectedMessage = "command outcome " + status.name().toLowerCase(Locale.ROOT) + ": " + outcome.code();
        assertEquals(expectedMessage, failure.getMessage());
        assertEquals(status, store.find("tenant", commandId).await().atMost(Duration.ofSeconds(5)).orElseThrow().status());
        Throwable replayFailure = assertThrows(type, () -> support.<String, String>execute(
            descriptor(), (ignored, input) -> commandId, "input").await().atMost(Duration.ofSeconds(5)));
        assertEquals(expectedMessage, replayFailure.getMessage());
        assertEquals(invocationsBefore + 1, operation.invocations);
    }

    private static CommandDescriptor descriptor() {
        return descriptor(Map.of("target", "orders"));
    }

    private static CommandDescriptor descriptor(CommandPolicy policy) {
        return descriptor(Map.of("target", "orders"), policy);
    }

    private static CommandDescriptor descriptor(Map<String, Object> configuration) {
        return descriptor(configuration, CommandPolicy.none());
    }

    private static CommandDescriptor descriptor(Map<String, Object> configuration, CommandPolicy policy) {
        NativeCommandSelector selector = new NativeCommandSelector(
            new ConnectorOperationIdentity(ConnectorProviderId.of("acme.search"), "write.document", ConnectorOperationKind.COMMAND, 1),
            1,
            policy);
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

    public static final class NativeProvider implements ConnectorProvider<Void> {
        private final NativeOperation operation;
        private final ConnectorExecutionCapabilities executionCapabilities;
        private int starts;

        public NativeProvider() {
            this(new NativeOperation());
            operation.outcome = new CommandOutcome.Succeeded<>(
                "bound-result", CommandConfirmation.none(), Set.of(), List.of());
        }

        private NativeProvider(NativeOperation operation) {
            this(operation, new ConnectorExecutionCapabilities(
                ConnectorExecutionStyle.PROVIDER_MANAGED, ConnectorConcurrencyScope.PROVIDER_MANAGED));
        }

        private NativeProvider(NativeOperation operation, ConnectorExecutionCapabilities executionCapabilities) {
            this.operation = operation;
            this.executionCapabilities = executionCapabilities;
        }

        @Override
        public ConnectorProviderId id() {
            return ConnectorProviderId.of("acme.search");
        }

        @Override
        public ConnectorProviderVersion version() {
            return new ConnectorProviderVersion(1, 0);
        }

        @Override
        public Collection<? extends ConnectorOperation> operations() {
            return List.of(operation);
        }

        @Override
        public org.pipelineframework.connector.ConnectorExecutionCapabilities executionCapabilities() {
            return executionCapabilities;
        }

        @Override
        public CompletionStage<Void> start(ConnectorRuntimeContext context) {
            starts++;
            return ConnectorCompletionStages.completed();
        }

    }

    private static final class NativeOperation implements CommandOperation<String, OperationConfig, String> {
        private CommandOutcome<String> outcome;
        private int invocations;
        private boolean failIfInvoked;
        private RuntimeException immediateFailure;
        private CompletionStage<CommandOutcome<String>> stageOverride;
        private boolean returnNullStage;
        private boolean retryRedriveSupported = true;
        private final List<CommandDispatchIdentity> dispatchIdentities = new java.util.ArrayList<>();

        @Override
        public String id() {
            return "write.document";
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
            dispatchIdentities.add(invocation.dispatchIdentity().orElseThrow());
            if (immediateFailure != null) {
                throw immediateFailure;
            }
            if (returnNullStage) {
                return null;
            }
            if (stageOverride != null) {
                return stageOverride;
            }
            return CompletableFuture.completedFuture(outcome);
        }

        private static ConnectorConfigSchema<OperationConfig> schema() {
            return ConnectorConfigSchema.record(OperationConfig.class, "acme.search.write.document", 1);
        }

        private CommandCapabilities declaredCapabilities() {
            return new CommandCapabilities(
                retryRedriveSupported, true, true, CommandExecutionPosture.AUTOMATED,
                CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, true, Set.of("ticket"));
        }
    }
}
