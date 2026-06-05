package org.pipelineframework;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
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
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionResultShapeResolver;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.EventWorkDispatcher;
import org.pipelineframework.orchestrator.InMemoryExecutionStateStore;
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
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.telemetry.AwaitReplayLifecycleEvent;
import org.pipelineframework.telemetry.PipelineTelemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    private PipelineTelemetry telemetry;

    @Mock
    private Instance<ExecutionStateStore> executionStateStores;

    @Mock
    private Instance<WorkDispatcher> workDispatchers;

    @Mock
    private Instance<DeadLetterPublisher> deadLetterPublishers;

    @Mock
    private ExecutionResultShapeResolver executionResultShapeResolver;

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
        coordinator.telemetry = telemetry;
        coordinator.checkpointPublicationService = checkpointPublicationService;
        coordinator.executionStateStores = executionStateStores;
        coordinator.workDispatchers = workDispatchers;
        coordinator.deadLetterPublishers = deadLetterPublishers;
        coordinator.executionInputPolicy = inputPolicy;
        coordinator.executionFailureHandler = failureHandler;
        coordinator.executionResultShapeResolver = executionResultShapeResolver;
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
            ExecutionResultShape.SINGLE,
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
        assertEquals(ExecutionResultShape.SINGLE, captor.getValue().resultShape());
    }

    @Test
    void executePipelineAsyncPersistsMultiInputShapeForStreamingPayload() {
        configureQueueModeDefaults();
        when(executionStateStore.createOrGetExecution(any()))
            .thenReturn(Uni.createFrom().item(new CreateExecutionResult(
                createRecord("tenant-1", "exec-multi", "key-multi", ExecutionResultShape.MATERIALIZED_MULTI),
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
    void executePipelineAsyncPersistsResolvedMaterializedMultiResultShape() {
        configureQueueModeDefaults();
        when(executionResultShapeResolver.resolve()).thenReturn(ExecutionResultShape.MATERIALIZED_MULTI);
        when(executionStateStore.createOrGetExecution(any()))
            .thenReturn(Uni.createFrom().item(new CreateExecutionResult(
                createRecord("tenant-1", "exec-resolved", "key-resolved", ExecutionResultShape.MATERIALIZED_MULTI),
                true)));

        coordinator.executePipelineAsync("input", "tenant-1", null, false)
            .await().indefinitely();

        ArgumentCaptor<org.pipelineframework.orchestrator.ExecutionCreateCommand> captor =
            ArgumentCaptor.forClass(org.pipelineframework.orchestrator.ExecutionCreateCommand.class);
        verify(executionStateStore).createOrGetExecution(captor.capture());
        assertEquals(ExecutionResultShape.MATERIALIZED_MULTI, captor.getValue().resultShape());
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
                org.mockito.ArgumentMatchers.eq("unit-1"),
                org.mockito.ArgumentMatchers.eq(0),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
        when(awaitCoordinator.getUnit("tenant-1", "unit-1"))
            .thenReturn(Uni.createFrom().item(awaitUnit("unit-1", AwaitUnitStatus.WAITING_EXTERNAL, 1, 0, false, null)));

        coordinator.processExecutionWorkItem(
                new ExecutionWorkItem("tenant-1", "exec-await"),
                record -> Multi.createFrom().failure(new AwaitSuspendedException(
                    "tenant-1",
                    "exec-await",
                    "unit-1",
                    0)))
            .await().indefinitely();

        verify(executionStateStore).markWaitingExternal(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-await"),
            org.mockito.ArgumentMatchers.eq(0L),
            org.mockito.ArgumentMatchers.eq("exec-await:0:0"),
            org.mockito.ArgumentMatchers.eq("unit-1"),
            org.mockito.ArgumentMatchers.eq(0),
            org.mockito.ArgumentMatchers.anyLong());
        verify(executionStateStore, never()).markAwaitCompleted(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyInt(),
            org.mockito.ArgumentMatchers.anyLong());
        verify(workDispatcher, never()).enqueueNow(any());
    }

    @Test
    void processExecutionWorkItemReleasesResumeWhenAwaitUnitCompletedBeforeWaitingPersists() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.leaseMs()).thenReturn(1000L);
        ExecutionRecord<Object, Object> claimed = createRecord("tenant-1", "exec-await", "key-await");
        ExecutionRecord<Object, Object> waiting = new ExecutionRecord<>(
            "tenant-1",
            "exec-await",
            "key-await",
            ExecutionResultShape.SINGLE,
            ExecutionStatus.WAITING_EXTERNAL,
            1L,
            2,
            0,
            null,
            0L,
            Long.MAX_VALUE,
            "exec-await:0:0",
            "input",
            "unit-1",
            null,
            null,
            null,
            1L,
            2L,
            99999999L);
        ExecutionRecord<Object, Object> resumed = new ExecutionRecord<>(
            "tenant-1",
            "exec-await",
            "key-await",
            ExecutionResultShape.SINGLE,
            ExecutionStatus.QUEUED,
            2L,
            3,
            0,
            null,
            0L,
            3L,
            "exec-await:0:0",
            "input",
            "unit-1",
            null,
            null,
            null,
            1L,
            3L,
            99999999L);
        when(executionStateStore.claimLease(any(), any(), any(), org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
        when(executionStateStore.markWaitingExternal(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-await"),
                org.mockito.ArgumentMatchers.eq(0L),
                org.mockito.ArgumentMatchers.eq("exec-await:0:0"),
                org.mockito.ArgumentMatchers.eq("unit-1"),
                org.mockito.ArgumentMatchers.eq(2),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(waiting)));
        when(awaitCoordinator.getUnit("tenant-1", "unit-1"))
            .thenReturn(Uni.createFrom().item(awaitUnit("unit-1", AwaitUnitStatus.COMPLETED, 10, 10, true, "interaction-1")));
        when(executionStateStore.markAwaitCompleted(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-await"),
                org.mockito.ArgumentMatchers.eq("unit-1"),
                org.mockito.ArgumentMatchers.eq(3),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(resumed)));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

        coordinator.processExecutionWorkItem(
                new ExecutionWorkItem("tenant-1", "exec-await"),
                record -> Multi.createFrom().failure(new AwaitSuspendedException(
                    "tenant-1",
                    "exec-await",
                    "unit-1",
                    2)))
            .await().indefinitely();

        verify(executionStateStore).markAwaitCompleted(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-await"),
            org.mockito.ArgumentMatchers.eq("unit-1"),
            org.mockito.ArgumentMatchers.eq(3),
            org.mockito.ArgumentMatchers.anyLong());
        verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-1", "exec-await"));
        ArgumentCaptor<AwaitReplayLifecycleEvent> lifecycleCaptor =
            ArgumentCaptor.forClass(AwaitReplayLifecycleEvent.class);
        verify(telemetry, org.mockito.Mockito.times(2)).recordAwaitLifecycle(lifecycleCaptor.capture());
        assertEquals(
            List.of(AwaitReplayLifecycleEvent.EXECUTION_WAITING, AwaitReplayLifecycleEvent.RESUME_RELEASED),
            lifecycleCaptor.getAllValues().stream().map(AwaitReplayLifecycleEvent::eventName).toList());
    }

    @Test
    void processExecutionWorkItemMarksWaitingExternalWhenAwaitSuspensionIsWrapped() {
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
                org.mockito.ArgumentMatchers.eq("unit-1"),
                org.mockito.ArgumentMatchers.eq(0),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
        when(awaitCoordinator.getUnit("tenant-1", "unit-1"))
            .thenReturn(Uni.createFrom().item(awaitUnit("unit-1", AwaitUnitStatus.WAITING_EXTERNAL, 1, 0, false, null)));

        coordinator.processExecutionWorkItem(
                new ExecutionWorkItem("tenant-1", "exec-await"),
                record -> Multi.createFrom().failure(new IllegalStateException(
                    "wrapped await failure",
                    new AwaitSuspendedException("tenant-1", "exec-await", "unit-1", 0))))
            .await().indefinitely();

        verify(executionStateStore).markWaitingExternal(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-await"),
            org.mockito.ArgumentMatchers.eq(0L),
            org.mockito.ArgumentMatchers.eq("exec-await:0:0"),
            org.mockito.ArgumentMatchers.eq("unit-1"),
            org.mockito.ArgumentMatchers.eq(0),
            org.mockito.ArgumentMatchers.anyLong());
        verify(workDispatcher, never()).enqueueNow(any());
    }

    @Test
    void processExecutionWorkItemFailsWhenWaitingExternalTransitionCannotBePersisted() {
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
                org.mockito.ArgumentMatchers.eq("unit-1"),
                org.mockito.ArgumentMatchers.eq(0),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.empty()));
        when(orchestratorConfig.maxRetries()).thenReturn(0);
        when(executionStateStore.markTerminalFailure(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-await"),
                org.mockito.ArgumentMatchers.eq(0L),
                org.mockito.ArgumentMatchers.eq(ExecutionStatus.FAILED),
                org.mockito.ArgumentMatchers.eq("exec-await:0:0"),
                org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.contains("WAITING_EXTERNAL"),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
        when(deadLetterPublisher.publish(any())).thenReturn(Uni.createFrom().voidItem());

        coordinator.processExecutionWorkItem(
                new ExecutionWorkItem("tenant-1", "exec-await"),
                record -> Multi.createFrom().failure(new AwaitSuspendedException(
                    "tenant-1",
                    "exec-await",
                    "unit-1",
                    0)))
            .await().indefinitely();

        verify(deadLetterPublisher).publish(any());
    }

    @Test
    void processExecutionWorkItemPersistsCollectedStreamingOutputs() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.leaseMs()).thenReturn(1000L);
        ExecutionRecord<Object, Object> claimed = createRecord(
            "tenant-1",
            "exec-stream",
            "key-stream",
            ExecutionResultShape.MATERIALIZED_MULTI);
        ExecutionRecord<Object, Object> succeeded = new ExecutionRecord<>(
            "tenant-1",
            "exec-stream",
            "key-stream",
            ExecutionResultShape.MATERIALIZED_MULTI,
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
    void runAsyncExecutionAllowsSingleShapeWithZeroOrOneItems() {
        ExecutionRecord<Object, Object> single = createRecord("tenant-1", "exec-single", "key-single");

        List<?> none = runAsyncExecution(single, record -> Multi.createFrom().empty());
        List<?> one = runAsyncExecution(single, record -> Multi.createFrom().item("only"));

        assertTrue(none.isEmpty());
        assertEquals(List.of("only"), one);
    }

    @Test
    void runAsyncExecutionRejectsMultipleItemsForSingleShape() {
        ExecutionRecord<Object, Object> single = createRecord("tenant-1", "exec-single", "key-single");

        IllegalStateException error = assertThrows(IllegalStateException.class, () ->
            runAsyncExecution(single, record -> Multi.createFrom().items("a", "b")));

        assertTrue(error.getMessage().contains("SINGLE result shape"));
        assertTrue(error.getMessage().contains("exec-single"));
    }

    @Test
    void getExecutionResultReturnsStoredListForMaterializedMulti() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(executionStateStore.getExecution("tenant-1", "exec-multi"))
            .thenReturn(Uni.createFrom().item(Optional.of(succeededRecord(
                "tenant-1",
                "exec-multi",
                "key-multi",
                ExecutionResultShape.MATERIALIZED_MULTI,
                List.of("a", "b")))));

        Object result = coordinator.getExecutionResult("tenant-1", "exec-multi", String.class, true)
            .await().indefinitely();

        assertEquals(List.of("a", "b"), result);
    }

    @Test
    void getExecutionResultRejectsUnaryRetrievalForMaterializedMulti() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(executionStateStore.getExecution("tenant-1", "exec-multi"))
            .thenReturn(Uni.createFrom().item(Optional.of(succeededRecord(
                "tenant-1",
                "exec-multi",
                "key-multi",
                ExecutionResultShape.MATERIALIZED_MULTI,
                List.of("a", "b")))));

        IllegalStateException error = assertThrows(IllegalStateException.class, () ->
            coordinator.getExecutionResult("tenant-1", "exec-multi", String.class, false).await().indefinitely());

        assertTrue(error.getMessage().contains("materialized multi result"));
    }

    @Test
    void getExecutionResultRejectsCorruptSingleResultList() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(executionStateStore.getExecution("tenant-1", "exec-corrupt"))
            .thenReturn(Uni.createFrom().item(Optional.of(succeededRecord(
                "tenant-1",
                "exec-corrupt",
                "key-corrupt",
                ExecutionResultShape.SINGLE,
                List.of("a", "b")))));

        IllegalStateException error = assertThrows(IllegalStateException.class, () ->
            coordinator.getExecutionResult("tenant-1", "exec-corrupt", String.class, false).await().indefinitely());

        assertTrue(error.getMessage().contains("multiple terminal items"));
    }

    @Test
    void completeAwaitPreservesDeterministicAdmissionFailures() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(awaitCoordinator.complete(any()))
            .thenReturn(Uni.createFrom().failure(new org.pipelineframework.awaitable.AwaitInteractionNotFoundException("missing")));

        RuntimeException error = assertThrows(RuntimeException.class, () -> coordinator.completeAwait(new AwaitCompletionCommand(
            "tenant-1",
            "interaction-1",
            null,
            null,
            java.util.Map.of("value", "approved"),
            "user-1",
            System.currentTimeMillis())).await().indefinitely());

        assertInstanceOf(org.pipelineframework.awaitable.AwaitInteractionNotFoundException.class, error);
    }

    @Test
    void completeAwaitWrapsTransientAdmissionFailures() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(awaitCoordinator.complete(any()))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("store down")));

        IllegalStateException error = assertThrows(IllegalStateException.class, () -> coordinator.completeAwait(new AwaitCompletionCommand(
            "tenant-1",
            "interaction-1",
            null,
            null,
            java.util.Map.of("value", "approved"),
            "user-1",
            System.currentTimeMillis())).await().indefinitely());

        assertTrue(error.getMessage().contains("Failed completing await interaction"));
        assertFalse(error.getCause() == null);
        assertEquals("store down", error.getCause().getMessage());
    }

    @Test
    void completeAwaitMarksUnitCompleteAndEnqueuesExecution() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        AwaitInteractionRecord interaction = awaitRecord();
        AwaitUnitRecord completedUnit = awaitUnit("unit-1", AwaitUnitStatus.COMPLETED, null, 0, true, "interaction-1");
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
        when(awaitCoordinator.recordCompletion(org.mockito.ArgumentMatchers.eq(interaction), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(completedUnit));
        ExecutionRecord<Object, Object> resumed = createRecord("tenant-1", "exec-1", "key-1");
        when(executionStateStore.markAwaitCompleted(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-1"),
                org.mockito.ArgumentMatchers.eq("unit-1"),
                org.mockito.ArgumentMatchers.eq(3),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(resumed)));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

        AwaitCompletionResult result = coordinator.completeAwait(command).await().indefinitely();

        assertEquals("interaction-1", result.record().interactionId());
        verify(executionStateStore).markAwaitCompleted(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-1"),
            org.mockito.ArgumentMatchers.eq("unit-1"),
            org.mockito.ArgumentMatchers.eq(3),
            org.mockito.ArgumentMatchers.anyLong());
        verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-1", "exec-1"));
    }

    @Test
    void completeAwaitPropagatesNestedOutputRecordAndUsesUnitId() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        AwaitInteractionRecord interaction = new AwaitInteractionRecord(
            "tenant-1",
            "exec-1",
            "ProcessApprovalService",
            2,
            NestedDecisionEnvelope.Decision.class.getCanonicalName(),
            "interaction-1",
            "corr-1",
            "cause-1",
            "idem-1",
            1L,
            AwaitInteractionStatus.COMPLETED,
            java.util.Map.of("value", "request"),
            java.util.Map.of("value", "approved"),
            "unit-1",
            null,
            "user-1",
            null,
            null,
            "interaction-api",
            java.util.Map.of(),
            System.currentTimeMillis() + 1000,
            1L,
            2L,
            99999999L);
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
        when(awaitCoordinator.recordCompletion(org.mockito.ArgumentMatchers.eq(interaction), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(awaitUnit("unit-1", AwaitUnitStatus.COMPLETED, null, 0, true, "interaction-1")));
        ExecutionRecord<Object, Object> resumed = createRecord("tenant-1", "exec-1", "key-1");
        when(executionStateStore.markAwaitCompleted(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-1"),
                org.mockito.ArgumentMatchers.eq("unit-1"),
                org.mockito.ArgumentMatchers.eq(3),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(resumed)));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

        coordinator.completeAwait(command).await().indefinitely();

        verify(executionStateStore).markAwaitCompleted(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-1"),
            org.mockito.ArgumentMatchers.eq("unit-1"),
            org.mockito.ArgumentMatchers.eq(3),
            org.mockito.ArgumentMatchers.anyLong());
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
        when(awaitCoordinator.recordCompletion(org.mockito.ArgumentMatchers.eq(interaction), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(awaitUnit("unit-1", AwaitUnitStatus.COMPLETED, null, 0, true, "interaction-1")));
        ExecutionRecord<Object, Object> resumed = createRecord("tenant-1", "exec-1", "key-1");
        when(executionStateStore.markAwaitCompleted(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-1"),
                org.mockito.ArgumentMatchers.eq("unit-1"),
                org.mockito.ArgumentMatchers.eq(3),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(resumed)));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

        AwaitCompletionResult result = coordinator.completeAwait(command).await().indefinitely();

        assertTrue(result.duplicate());
        verify(executionStateStore).markAwaitCompleted(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-1"),
            org.mockito.ArgumentMatchers.eq("unit-1"),
            org.mockito.ArgumentMatchers.eq(3),
            org.mockito.ArgumentMatchers.anyLong());
        verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-1", "exec-1"));
    }

    @Test
    void completeAwaitDoesNotResumeUntilUnitIsComplete() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        AwaitInteractionRecord completed = itemAwaitRecord(0, AwaitInteractionStatus.COMPLETED, "approved");
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
        when(awaitCoordinator.recordCompletion(org.mockito.ArgumentMatchers.eq(completed), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(awaitUnit("unit-1", AwaitUnitStatus.WAITING_EXTERNAL, 2, 1, true, null)));

        AwaitCompletionResult result = coordinator.completeAwait(command).await().indefinitely();

        assertEquals(completed.interactionId(), result.record().interactionId());
        verify(executionStateStore, never()).markAwaitCompleted(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyInt(),
            org.mockito.ArgumentMatchers.anyLong());
        verify(workDispatcher, never()).enqueueNow(any());
    }

    @Test
    void completeAwaitMarksParentFailedWhenItemContinuationKeepsFailing() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        AwaitInteractionRecord completed = itemAwaitRecord(0, AwaitInteractionStatus.COMPLETED, "approved");
        AwaitCompletionCommand command = new AwaitCompletionCommand(
            "tenant-1",
            completed.interactionId(),
            null,
            null,
            java.util.Map.of("value", "approved"),
            "user-1",
            System.currentTimeMillis());
        AwaitUnitRecord unit = awaitUnit("unit-1", AwaitUnitStatus.COMPLETED, 1, 1, true, null);
        ExecutionRecord<Object, Object> waiting = new ExecutionRecord<>(
            "tenant-1",
            "exec-1",
            "key-1",
            ExecutionResultShape.MATERIALIZED_MULTI,
            ExecutionStatus.WAITING_EXTERNAL,
            7L,
            2,
            0,
            null,
            0L,
            0L,
            null,
            "input",
            "unit-1",
            null,
            null,
            null,
            1L,
            1L,
            99999999L);
        AwaitItemContinuationHandler failingHandler = new AwaitItemContinuationHandler() {
            @Override
            public Uni<Void> continueAwaitItem(
                AwaitInteractionRecord record,
                AwaitUnitRecord unit,
                int nextStepIndex,
                Optional<ExecutionRecord<Object, Object>> parent,
                long nowEpochMs) {
                return Uni.createFrom().failure(new IllegalStateException("segment failed"));
            }

            @Override
            public Uni<Void> releaseAwaitParentIfReady(
                ExecutionRecord<Object, Object> parent,
                AwaitUnitRecord unit,
                int nextStepIndex,
                long nowEpochMs) {
                return Uni.createFrom().voidItem();
            }
        };
        when(awaitCoordinator.complete(command))
            .thenReturn(Uni.createFrom().item(new AwaitCompletionResult(completed, false)));
        when(awaitCoordinator.recordCompletion(org.mockito.ArgumentMatchers.eq(completed), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(unit));
        when(executionStateStore.getExecution("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(Optional.of(waiting)));
        when(executionStateStore.markTerminalFailure(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-1"),
                org.mockito.ArgumentMatchers.eq(7L),
                org.mockito.ArgumentMatchers.eq(ExecutionStatus.FAILED),
                org.mockito.ArgumentMatchers.eq("await-item-continuation-failed:unit-1:0"),
                org.mockito.ArgumentMatchers.eq("AWAIT_ITEM_CONTINUATION_FAILED"),
                org.mockito.ArgumentMatchers.contains("segment failed"),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(waiting)));

        coordinator.completeAwait(command, failingHandler).await().indefinitely();

        verify(executionStateStore, timeout(1000)).markTerminalFailure(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-1"),
            org.mockito.ArgumentMatchers.eq(7L),
            org.mockito.ArgumentMatchers.eq(ExecutionStatus.FAILED),
            org.mockito.ArgumentMatchers.eq("await-item-continuation-failed:unit-1:0"),
            org.mockito.ArgumentMatchers.eq("AWAIT_ITEM_CONTINUATION_FAILED"),
            org.mockito.ArgumentMatchers.contains("segment failed"),
            org.mockito.ArgumentMatchers.anyLong());
        verify(workDispatcher, never()).enqueueNow(any());
    }

    @Test
    void completeAwaitResumesCompletedItemUnitAndEnqueuesExecution() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        AwaitInteractionRecord second = awaitRecord();
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
        when(awaitCoordinator.recordCompletion(org.mockito.ArgumentMatchers.eq(second), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(awaitUnit("unit-1", AwaitUnitStatus.COMPLETED, null, 0, false, "interaction-1")));
        ExecutionRecord<Object, Object> resumed = createRecord("tenant-1", "exec-1", "key-1");
        when(executionStateStore.markAwaitCompleted(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-1"),
                org.mockito.ArgumentMatchers.eq("unit-1"),
                org.mockito.ArgumentMatchers.eq(3),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(resumed)));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

        coordinator.completeAwait(command).await().indefinitely();

        verify(executionStateStore).markAwaitCompleted(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-1"),
            org.mockito.ArgumentMatchers.eq("unit-1"),
            org.mockito.ArgumentMatchers.eq(3),
            org.mockito.ArgumentMatchers.anyLong());
        verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-1", "exec-1"));
    }

    @Test
    void recordAwaitItemContinuationStoresChildrenAndReleasesParentAtAggregateBoundaryInOrder() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        coordinator.executionStateStore = store;
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());
        long now = System.currentTimeMillis();
        CreateExecutionResult parent = store.createOrGetExecution(new org.pipelineframework.orchestrator.ExecutionCreateCommand(
                "tenant-1",
                "parent-key",
                new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input"),
                ExecutionResultShape.MATERIALIZED_MULTI,
                now,
                now + 100000))
            .await().indefinitely();
        store.markWaitingExternal(
                "tenant-1",
                parent.record().executionId(),
                parent.record().version(),
                "transition",
                "unit-1",
                2,
                now)
            .await().indefinitely();
        AwaitUnitRecord unit = awaitUnit("unit-1", AwaitUnitStatus.COMPLETED, 2, 2, true, null);

        coordinator.recordAwaitItemContinuation(
                itemAwaitRecord(parent.record().executionId(), 1, AwaitInteractionStatus.COMPLETED, "second"),
                unit,
                4,
                new ExecutionInputSnapshot(ExecutionInputShape.UNI, "second-normalized"),
                List.of("out-1"),
                now)
            .await().indefinitely();
        verify(workDispatcher, never()).enqueueNow(any());

        coordinator.recordAwaitItemContinuation(
                itemAwaitRecord(parent.record().executionId(), 0, AwaitInteractionStatus.COMPLETED, "first"),
                unit,
                4,
                new ExecutionInputSnapshot(ExecutionInputShape.UNI, "first-normalized"),
                List.of("out-0"),
                now)
            .await().indefinitely();

        ExecutionRecord<Object, Object> resumed = store.getExecution("tenant-1", parent.record().executionId())
            .await().indefinitely()
            .orElseThrow();
        assertEquals(ExecutionStatus.QUEUED, resumed.status());
        assertEquals(4, resumed.currentStepIndex());
        assertNull(resumed.awaitUnitId());
        assertInstanceOf(ExecutionInputSnapshot.class, resumed.inputPayload());
        ExecutionInputSnapshot snapshot = (ExecutionInputSnapshot) resumed.inputPayload();
        assertEquals(ExecutionInputShape.MULTI, snapshot.shape());
        assertEquals(List.of("out-0", "out-1"), snapshot.payload());
        ExecutionRecord<Object, Object> firstChild = store.getExecutionByKey(
                "tenant-1",
                "parent-key:await-item:unit-1:0")
            .await().indefinitely()
            .orElseThrow();
        assertInstanceOf(ExecutionInputSnapshot.class, firstChild.inputPayload());
        ExecutionInputSnapshot firstChildInput = (ExecutionInputSnapshot) firstChild.inputPayload();
        assertEquals(ExecutionInputShape.UNI, firstChildInput.shape());
        assertEquals("first-normalized", firstChildInput.payload());
        verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-1", parent.record().executionId()));
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
            ExecutionResultShape.SINGLE,
            ExecutionStatus.WAITING_EXTERNAL,
            7L,
            2,
            0,
            null,
            0L,
            0L,
            null,
            "input",
            "unit-1",
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
        when(executionResultShapeResolver.resolve()).thenReturn(ExecutionResultShape.SINGLE);
    }

    private ExecutionRecord<Object, Object> createRecord(String tenantId, String executionId, String executionKey) {
        return createRecord(tenantId, executionId, executionKey, ExecutionResultShape.SINGLE);
    }

    private ExecutionRecord<Object, Object> createRecord(
        String tenantId,
        String executionId,
        String executionKey,
        ExecutionResultShape resultShape) {
        return new ExecutionRecord<>(
            tenantId,
            executionId,
            executionKey,
            resultShape,
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
            null,
            1L,
            1L,
            99999999L);
    }

    private ExecutionRecord<Object, Object> succeededRecord(
        String tenantId,
        String executionId,
        String executionKey,
        ExecutionResultShape resultShape,
        List<?> resultPayload) {
        return new ExecutionRecord<>(
            tenantId,
            executionId,
            executionKey,
            resultShape,
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
            resultPayload,
            null,
            null,
            1L,
            1L,
            99999999L);
    }

    @SuppressWarnings("unchecked")
    private List<?> runAsyncExecution(
        ExecutionRecord<Object, Object> record,
        Function<ExecutionRecord<Object, Object>, Multi<?>> executeStreaming) {
        try {
            Method method = QueueAsyncCoordinator.class.getDeclaredMethod(
                "runAsyncExecution",
                ExecutionRecord.class,
                Function.class);
            method.setAccessible(true);
            return ((Uni<List<?>>) method.invoke(coordinator, record, executeStreaming)).await().indefinitely();
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Failed invoking runAsyncExecution", e);
        }
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
            "unit-1",
            null,
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

    private AwaitInteractionRecord itemAwaitRecord(
        int itemIndex,
        AwaitInteractionStatus status,
        String decision) {
        return itemAwaitRecord("exec-1", itemIndex, status, decision);
    }

    private AwaitInteractionRecord itemAwaitRecord(
        String executionId,
        int itemIndex,
        AwaitInteractionStatus status,
        String decision) {
        return new AwaitInteractionRecord(
            "tenant-1",
            executionId,
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
            "unit-1",
            itemIndex,
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

    private AwaitUnitRecord awaitUnit(
        String unitId,
        AwaitUnitStatus status,
        Integer expectedItemCount,
        int completedItemCount,
        boolean dispatchComplete,
        String primaryInteractionId) {
        return new AwaitUnitRecord(
            "tenant-1",
            unitId,
            "exec-1",
            "AwaitPaymentProvider",
            2,
            "ONE_TO_ONE",
            1L,
            status,
            primaryInteractionId,
            expectedItemCount,
            completedItemCount,
            dispatchComplete,
            1L,
            1L,
            99999999L);
    }

    private record Decision(String value) {
    }

    private static final class NestedDecisionEnvelope {
        public record Decision(String value) {
        }
    }
}
