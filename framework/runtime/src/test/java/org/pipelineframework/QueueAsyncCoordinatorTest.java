package org.pipelineframework;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.EventWorkDispatcher;
import org.pipelineframework.orchestrator.LoggingDeadLetterPublisher;
import org.pipelineframework.orchestrator.OrchestratorIdempotencyPolicy;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.DynamoExecutionStateStore;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitSuspendedException;
import org.pipelineframework.checkpoint.CheckpointPublicationService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class QueueAsyncCoordinatorTest {

    private QueueAsyncCoordinator coordinator;
    private ExecutionInputPolicy inputPolicy;
    private ExecutionFailureHandler failureHandler;

    @Mock
    private PipelineOrchestratorConfig orchestratorConfig;

    @Mock
    private ExecutionStateStore executionStateStore;

    @Mock
    private WorkDispatcher workDispatcher;

    @Mock
    private DeadLetterPublisher deadLetterPublisher;

    @Mock
    private AwaitCoordinator awaitCoordinator;

    @Mock
    private CheckpointPublicationService checkpointPublicationService;

    @Mock
    private Instance<ExecutionStateStore> executionStateStores;

    @Mock
    private Instance<WorkDispatcher> workDispatchers;

    @Mock
    private Instance<DeadLetterPublisher> deadLetterPublishers;

    @BeforeEach
    void setUp() {
        inputPolicy = new ExecutionInputPolicy();
        inputPolicy.orchestratorConfig = orchestratorConfig;

        failureHandler = new ExecutionFailureHandler();
        failureHandler.orchestratorConfig = orchestratorConfig;

        coordinator = new QueueAsyncCoordinator();
        coordinator.orchestratorConfig = orchestratorConfig;
        coordinator.executionStateStore = executionStateStore;
        coordinator.workDispatcher = workDispatcher;
        coordinator.deadLetterPublisher = deadLetterPublisher;
        coordinator.awaitCoordinator = awaitCoordinator;
        coordinator.checkpointPublicationService = checkpointPublicationService;
        coordinator.executionStateStores = executionStateStores;
        coordinator.workDispatchers = workDispatchers;
        coordinator.deadLetterPublishers = deadLetterPublishers;
        coordinator.executionInputPolicy = inputPolicy;
        coordinator.executionFailureHandler = failureHandler;
    }

    @Test
    void executePipelineAsyncRequiresQueueMode() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.SYNC);

        Uni<?> result = coordinator.executePipelineAsync("input", "tenant-1", "idem-1", false);

        assertThrows(IllegalStateException.class, () -> result.await().indefinitely());
    }

    @Test
    void executePipelineAsyncRejectsStreamingOutputFlagInQueueMode() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);

        Uni<?> result = coordinator.executePipelineAsync("input", "tenant-1", "idem-1", true);

        IllegalStateException error = assertThrows(IllegalStateException.class, () -> result.await().indefinitely());
        assertTrue(error.getMessage().contains("does not support streaming pipeline outputs"));
    }

    @Test
    void initializeQueueModeFailsFastWhenSelectedProviderReportsStartupError() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.stateProvider()).thenReturn("dynamo");
        when(orchestratorConfig.dispatcherProvider()).thenReturn("event");
        when(orchestratorConfig.dlqProvider()).thenReturn("log");
        when(orchestratorConfig.strictStartup()).thenReturn(true);

        when(executionStateStores.stream()).thenReturn(Stream.of(new DynamoExecutionStateStore()));
        when(workDispatchers.stream()).thenReturn(Stream.of(new EventWorkDispatcher()));
        when(deadLetterPublishers.stream()).thenReturn(Stream.of(new LoggingDeadLetterPublisher()));

        IllegalStateException error = assertThrows(IllegalStateException.class, coordinator::initializeQueueMode);
        assertTrue(error.getMessage().contains("ExecutionStateStore(dynamo)"));
    }

    @Test
    void getExecutionStatusReturnsRecordStateInQueueMode() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        ExecutionRecord<Object, Object> record = new ExecutionRecord<>(
            "tenant-1",
            "exec-1",
            "key-1",
            ExecutionStatus.RUNNING,
            1L,
            0,
            0,
            null,
            0L,
            0L,
            null,
            "input",
            null,
            null,
            null,
            1L,
            1L,
            99999999L);
        when(executionStateStore.getExecution("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));

        ExecutionStatusDto dto = coordinator.getExecutionStatus("tenant-1", "exec-1").await().indefinitely();

        assertNotNull(dto);
        assertEquals("exec-1", dto.executionId());
        assertEquals(ExecutionStatus.RUNNING, dto.status());
        verify(executionStateStore).getExecution("tenant-1", "exec-1");
    }

    @Test
    void executePipelineAsyncPersistsUniInputShapeForPlainArrayPayload() {
        configureQueueModeDefaults();
        when(executionStateStore.createOrGetExecution(any()))
            .thenReturn(Uni.createFrom().item(new CreateExecutionResult(
                createRecord("tenant-1", "exec-uni", "key-uni"),
                true)));

        coordinator.executePipelineAsync(java.util.List.of("a", "b"), "tenant-1", null, false)
            .await().indefinitely();

        ArgumentCaptor<org.pipelineframework.orchestrator.ExecutionCreateCommand> captor =
            ArgumentCaptor.forClass(org.pipelineframework.orchestrator.ExecutionCreateCommand.class);
        verify(executionStateStore).createOrGetExecution(captor.capture());
        Object persisted = captor.getValue().inputPayload();
        assertTrue(persisted instanceof ExecutionInputSnapshot);
        ExecutionInputSnapshot snapshot = (ExecutionInputSnapshot) persisted;
        assertEquals(ExecutionInputShape.UNI, snapshot.shape());
        assertEquals(java.util.List.of("a", "b"), snapshot.payload());
    }

    @Test
    void executePipelineAsyncPersistsMultiInputShapeForStreamingPayload() {
        configureQueueModeDefaults();
        when(executionStateStore.createOrGetExecution(any()))
            .thenReturn(Uni.createFrom().item(new CreateExecutionResult(
                createRecord("tenant-1", "exec-multi", "key-multi"),
                true)));

        coordinator.executePipelineAsync(Multi.createFrom().items("x", "y"), "tenant-1", null, false)
            .await().indefinitely();

        ArgumentCaptor<org.pipelineframework.orchestrator.ExecutionCreateCommand> captor =
            ArgumentCaptor.forClass(org.pipelineframework.orchestrator.ExecutionCreateCommand.class);
        verify(executionStateStore).createOrGetExecution(captor.capture());
        Object persisted = captor.getValue().inputPayload();
        assertTrue(persisted instanceof ExecutionInputSnapshot);
        ExecutionInputSnapshot snapshot = (ExecutionInputSnapshot) persisted;
        assertEquals(ExecutionInputShape.MULTI, snapshot.shape());
        assertEquals(java.util.List.of("x", "y"), snapshot.payload());
    }

    @Test
    void sweepRedispatchesPersistedDueExecutions() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.sweepLimit()).thenReturn(100);
        when(awaitCoordinator.findTimedOut(org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.eq(100)))
            .thenReturn(Uni.createFrom().item(java.util.List.of()));
        when(executionStateStore.findDueExecutions(org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyInt()))
            .thenReturn(Uni.createFrom().item(java.util.List.of(
                createRecord("tenant-a", "exec-5", "key-5"),
                createRecord("tenant-b", "exec-6", "key-6"))));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

        coordinator.sweepDueExecutions();

        ArgumentCaptor<ExecutionWorkItem> itemCaptor = ArgumentCaptor.forClass(ExecutionWorkItem.class);
        verify(workDispatcher, timeout(500).times(2)).enqueueNow(itemCaptor.capture());
    }

    @Test
    void processExecutionWorkItemMarksWaitingExternalWhenAwaitSuspends() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.leaseMs()).thenReturn(1000L);
        ExecutionRecord<Object, Object> claimed = createRecord("tenant-1", "exec-await", "key-await");
        when(executionStateStore.claimLease(any(), any(), any(), org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
        when(executionStateStore.markWaitingExternal(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-await"),
                org.mockito.ArgumentMatchers.eq(0L),
                org.mockito.ArgumentMatchers.eq("exec-await:0:0"),
                org.mockito.ArgumentMatchers.eq("interaction-1"),
                org.mockito.ArgumentMatchers.eq(0),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(claimed)));

        coordinator.processExecutionWorkItem(
                new ExecutionWorkItem("tenant-1", "exec-await"),
                record -> Multi.createFrom().failure(new AwaitSuspendedException(
                    "tenant-1",
                    "exec-await",
                    "interaction-1",
                    0)))
            .await().indefinitely();

        verify(executionStateStore).markWaitingExternal(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-await"),
            org.mockito.ArgumentMatchers.eq(0L),
            org.mockito.ArgumentMatchers.eq("exec-await:0:0"),
            org.mockito.ArgumentMatchers.eq("interaction-1"),
            org.mockito.ArgumentMatchers.eq(0),
            org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    void processExecutionWorkItemPersistsCollectedStreamingOutputs() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.leaseMs()).thenReturn(1000L);
        ExecutionRecord<Object, Object> claimed = createRecord("tenant-1", "exec-stream", "key-stream");
        ExecutionRecord<Object, Object> succeeded = new ExecutionRecord<>(
            "tenant-1",
            "exec-stream",
            "key-stream",
            ExecutionStatus.SUCCEEDED,
            1L,
            0,
            0,
            null,
            0L,
            0L,
            null,
            null,
            null,
            null,
            List.of("out-1", "out-2"),
            null,
            null,
            1L,
            1L,
            99999999L);
        when(executionStateStore.claimLease(any(), any(), any(), org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
        when(checkpointPublicationService.publishIfConfigured(org.mockito.ArgumentMatchers.eq(claimed), org.mockito.ArgumentMatchers.eq("out-1")))
            .thenReturn(Uni.createFrom().voidItem());
        when(executionStateStore.markSucceeded(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-stream"),
                org.mockito.ArgumentMatchers.eq(0L),
                org.mockito.ArgumentMatchers.eq("exec-stream:0:0"),
                org.mockito.ArgumentMatchers.eq(List.of("out-1", "out-2")),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(succeeded)));

        coordinator.processExecutionWorkItem(
                new ExecutionWorkItem("tenant-1", "exec-stream"),
                record -> Multi.createFrom().items("out-1", "out-2"))
            .await().indefinitely();

        verify(executionStateStore).markSucceeded(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-stream"),
            org.mockito.ArgumentMatchers.eq(0L),
            org.mockito.ArgumentMatchers.eq("exec-stream:0:0"),
            org.mockito.ArgumentMatchers.eq(List.of("out-1", "out-2")),
            org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    void completeAwaitStoresResumePayloadAndEnqueuesExecution() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        AwaitInteractionRecord interaction = awaitRecord();
        AwaitCompletionCommand command = new AwaitCompletionCommand(
            "tenant-1",
            "interaction-1",
            null,
            null,
            java.util.Map.of("value", "approved"),
            "user-1",
            System.currentTimeMillis());
        when(awaitCoordinator.complete(command))
            .thenReturn(Uni.createFrom().item(new AwaitCompletionResult(interaction, false)));
        ExecutionRecord<Object, Object> resumed = createRecord("tenant-1", "exec-1", "key-1");
        // Uses a Map payload intentionally so coerceAwaitPayload exercises PipelineJson conversion into Decision.
        when(executionStateStore.markAwaitCompleted(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-1"),
                org.mockito.ArgumentMatchers.eq("interaction-1"),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.eq(3),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(resumed)));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

        AwaitCompletionResult result = coordinator.completeAwait(command).await().indefinitely();

        assertEquals("interaction-1", result.record().interactionId());
        ArgumentCaptor<Object> payloadCaptor = ArgumentCaptor.forClass(Object.class);
        verify(executionStateStore).markAwaitCompleted(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-1"),
            org.mockito.ArgumentMatchers.eq("interaction-1"),
            payloadCaptor.capture(),
            org.mockito.ArgumentMatchers.eq(3),
            org.mockito.ArgumentMatchers.anyLong());
        assertEquals(new Decision("approved"), payloadCaptor.getValue());
        verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-1", "exec-1"));
    }

    @Test
    void completeAwaitRetriesResumeForDuplicateCompletedInteraction() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        AwaitInteractionRecord interaction = awaitRecord();
        AwaitCompletionCommand command = new AwaitCompletionCommand(
            "tenant-1",
            "interaction-1",
            null,
            null,
            java.util.Map.of("value", "approved"),
            "user-1",
            System.currentTimeMillis());
        when(awaitCoordinator.complete(command))
            .thenReturn(Uni.createFrom().item(new AwaitCompletionResult(interaction, true)));
        ExecutionRecord<Object, Object> resumed = createRecord("tenant-1", "exec-1", "key-1");
        when(executionStateStore.markAwaitCompleted(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-1"),
                org.mockito.ArgumentMatchers.eq("interaction-1"),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.eq(3),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(resumed)));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

        AwaitCompletionResult result = coordinator.completeAwait(command).await().indefinitely();

        assertTrue(result.duplicate());
        verify(executionStateStore).markAwaitCompleted(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-1"),
            org.mockito.ArgumentMatchers.eq("interaction-1"),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.eq(3),
            org.mockito.ArgumentMatchers.anyLong());
        verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-1", "exec-1"));
    }

    @Test
    void completeAwaitBarrierWaitsForAllItems() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        AwaitInteractionRecord completed = barrierAwaitRecord(0, AwaitInteractionStatus.COMPLETED, "approved");
        AwaitInteractionRecord pending = barrierAwaitRecord(1, AwaitInteractionStatus.DISPATCHED, null);
        AwaitCompletionCommand command = new AwaitCompletionCommand(
            "tenant-1",
            completed.interactionId(),
            null,
            null,
            java.util.Map.of("value", "approved"),
            "user-1",
            System.currentTimeMillis());
        when(awaitCoordinator.complete(command))
            .thenReturn(Uni.createFrom().item(new AwaitCompletionResult(completed, false)));
        when(awaitCoordinator.findByBarrier("tenant-1", "exec-1", 2, "barrier-1"))
            .thenReturn(Uni.createFrom().item(List.of(completed, pending)));

        AwaitCompletionResult result = coordinator.completeAwait(command).await().indefinitely();

        assertEquals(completed.interactionId(), result.record().interactionId());
        verify(executionStateStore, never()).markAwaitCompleted(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyInt(),
            org.mockito.ArgumentMatchers.anyLong());
        verify(workDispatcher, never()).enqueueNow(any());
    }

    @Test
    void completeAwaitBarrierReconstructsOrderedResumePayloadAndEnqueuesExecution() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        AwaitInteractionRecord first = barrierAwaitRecord(0, AwaitInteractionStatus.COMPLETED, "approved");
        AwaitInteractionRecord second = barrierAwaitRecord(1, AwaitInteractionStatus.COMPLETED, "review");
        AwaitCompletionCommand command = new AwaitCompletionCommand(
            "tenant-1",
            second.interactionId(),
            null,
            null,
            java.util.Map.of("value", "review"),
            "user-1",
            System.currentTimeMillis());
        when(awaitCoordinator.complete(command))
            .thenReturn(Uni.createFrom().item(new AwaitCompletionResult(second, false)));
        when(awaitCoordinator.findByBarrier("tenant-1", "exec-1", 2, "barrier-1"))
            .thenReturn(Uni.createFrom().item(List.of(second, first)));
        ExecutionRecord<Object, Object> resumed = createRecord("tenant-1", "exec-1", "key-1");
        when(executionStateStore.markAwaitCompleted(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-1"),
                org.mockito.ArgumentMatchers.eq("barrier-1"),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.eq(3),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(resumed)));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

        coordinator.completeAwait(command).await().indefinitely();

        ArgumentCaptor<Object> payloadCaptor = ArgumentCaptor.forClass(Object.class);
        verify(executionStateStore).markAwaitCompleted(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-1"),
            org.mockito.ArgumentMatchers.eq("barrier-1"),
            payloadCaptor.capture(),
            org.mockito.ArgumentMatchers.eq(3),
            org.mockito.ArgumentMatchers.anyLong());
        assertEquals(List.of(new Decision("approved"), new Decision("review")), payloadCaptor.getValue());
        verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-1", "exec-1"));
    }

    @Test
    void sweepTimesOutAwaitInteractionsAndFailsOwningExecution() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.sweepLimit()).thenReturn(100);
        AwaitInteractionRecord interaction = awaitRecord();
        ExecutionRecord<Object, Object> waiting = new ExecutionRecord<>(
            "tenant-1",
            "exec-1",
            "key-1",
            ExecutionStatus.WAITING_EXTERNAL,
            7L,
            2,
            0,
            null,
            0L,
            0L,
            null,
            "input",
            "interaction-1",
            null,
            null,
            null,
            null,
            1L,
            1L,
            99999999L);
        when(awaitCoordinator.findTimedOut(org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.eq(100)))
            .thenReturn(Uni.createFrom().item(java.util.List.of(interaction)));
        when(awaitCoordinator.markTimedOut(org.mockito.ArgumentMatchers.eq(interaction), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(interaction)));
        when(executionStateStore.getExecution("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(Optional.of(waiting)));
        when(executionStateStore.markTerminalFailure(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-1"),
                org.mockito.ArgumentMatchers.eq(7L),
                org.mockito.ArgumentMatchers.eq(ExecutionStatus.FAILED),
                org.mockito.ArgumentMatchers.eq("exec-1:2:0"),
                org.mockito.ArgumentMatchers.eq("AWAIT_TIMEOUT"),
                org.mockito.ArgumentMatchers.contains("interaction-1"),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(waiting)));
        when(executionStateStore.findDueExecutions(org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.eq(100)))
            .thenReturn(Uni.createFrom().item(java.util.List.of()));

        coordinator.sweepDueExecutions();

        verify(executionStateStore, timeout(500)).markTerminalFailure(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-1"),
            org.mockito.ArgumentMatchers.eq(7L),
            org.mockito.ArgumentMatchers.eq(ExecutionStatus.FAILED),
            org.mockito.ArgumentMatchers.eq("exec-1:2:0"),
            org.mockito.ArgumentMatchers.eq("AWAIT_TIMEOUT"),
            org.mockito.ArgumentMatchers.contains("interaction-1"),
            org.mockito.ArgumentMatchers.anyLong());
    }

    private void configureQueueModeDefaults() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.executionTtlDays()).thenReturn(7);
        when(orchestratorConfig.idempotencyPolicy()).thenReturn(OrchestratorIdempotencyPolicy.OPTIONAL_CLIENT_KEY);
    }

    private ExecutionRecord<Object, Object> createRecord(String tenantId, String executionId, String executionKey) {
        return new ExecutionRecord<>(
            tenantId,
            executionId,
            executionKey,
            ExecutionStatus.QUEUED,
            0L,
            0,
            0,
            null,
            0L,
            0L,
            null,
            null,
            null,
            null,
            null,
            1L,
            1L,
            99999999L);
    }

    private AwaitInteractionRecord awaitRecord() {
        return new AwaitInteractionRecord(
            "tenant-1",
            "exec-1",
            "ProcessApprovalService",
            2,
            Decision.class.getName(),
            "interaction-1",
            "corr-1",
            "cause-1",
            "idem-1",
            1L,
            AwaitInteractionStatus.COMPLETED,
            java.util.Map.of("value", "request"),
            java.util.Map.of("value", "approved"),
            "user-1",
            null,
            null,
            "interaction-api",
            java.util.Map.of(),
            System.currentTimeMillis() + 1000,
            1L,
            2L,
            99999999L);
    }

    private AwaitInteractionRecord barrierAwaitRecord(
        int itemIndex,
        AwaitInteractionStatus status,
        String decision) {
        return new AwaitInteractionRecord(
            "tenant-1",
            "exec-1",
            "AwaitPaymentProvider",
            2,
            Decision.class.getName(),
            "interaction-" + itemIndex,
            "corr-" + itemIndex,
            "cause-" + itemIndex,
            "idem-" + itemIndex,
            1L,
            status,
            java.util.Map.of("value", "request-" + itemIndex),
            decision == null ? null : java.util.Map.of("value", decision),
            "barrier-1",
            itemIndex,
            2,
            "user-1",
            null,
            null,
            "kafka",
            java.util.Map.of(),
            System.currentTimeMillis() + 1000,
            1L,
            2L,
            99999999L);
    }

    private record Decision(String value) {
    }
}
