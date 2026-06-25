package org.pipelineframework;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
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
import org.pipelineframework.orchestrator.ControlPlaneAdmissionException;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionRequest;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionOperation;
import org.pipelineframework.orchestrator.ControlPlaneTransitionAdmission;
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionRedriveResult;
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
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.LocalControlPlaneAdmissionPolicy;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.TransitionWorkerExecutionMode;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerOutcome;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.DynamoExecutionStateStore;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitLiveCompletionRegistry;
import org.pipelineframework.awaitable.AwaitSuspendedException;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.objectpublish.ObjectPublishCompletionService;
import org.pipelineframework.telemetry.AwaitReplayLifecycleEvent;
import org.pipelineframework.telemetry.PipelineTelemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class QueueAsyncCoordinatorTest {

    private QueueAsyncCoordinator coordinator;
    private ExecutionInputPolicy inputPolicy;
    private ExecutionFailureHandler failureHandler;
    private JsonTransitionPayloadCodec payloadCodec;

    @Mock
    private PipelineOrchestratorConfig orchestratorConfig;

    @Mock
    private PipelineOrchestratorConfig.WorkerConfig workerConfig;

    @Mock
    private ExecutionStateStore executionStateStore;

    @Mock
    private WorkDispatcher workDispatcher;

    @Mock
    private DeadLetterPublisher deadLetterPublisher;

    @Mock
    private AwaitCoordinator awaitCoordinator;

    @Mock
    private AwaitLiveCompletionRegistry awaitLiveCompletionRegistry;

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
        payloadCodec = new JsonTransitionPayloadCodec();

        coordinator = new QueueAsyncCoordinator();
        coordinator.orchestratorConfig = orchestratorConfig;
        coordinator.executionStateStore = executionStateStore;
        coordinator.workDispatcher = workDispatcher;
        coordinator.deadLetterPublisher = deadLetterPublisher;
        coordinator.awaitCoordinator = awaitCoordinator;
        coordinator.awaitLiveCompletionRegistry = awaitLiveCompletionRegistry;
        coordinator.transitionWorkerExecutor = new TransitionWorkerExecutor(
            orchestratorConfig,
            new PipelineInvocationRuntime());
        coordinator.transitionPayloadCodec = payloadCodec;
        coordinator.telemetry = telemetry;
        coordinator.checkpointPublicationService = checkpointPublicationService;
        coordinator.executionStateStores = executionStateStores;
        coordinator.workDispatchers = workDispatchers;
        coordinator.deadLetterPublishers = deadLetterPublishers;
        coordinator.executionInputPolicy = inputPolicy;
        coordinator.executionFailureHandler = failureHandler;
        coordinator.executionResultShapeResolver = executionResultShapeResolver;
        lenient().when(orchestratorConfig.worker()).thenReturn(workerConfig);
        lenient().when(workerConfig.executionMode()).thenReturn(TransitionWorkerExecutionMode.SAME_THREAD);
        lenient().when(workerConfig.maxInFlight()).thenReturn(64);
        lenient().when(workerConfig.saturatedDelay()).thenReturn(Duration.ofSeconds(1));
        lenient().when(awaitCoordinator.importSuspension(any())).thenReturn(Uni.createFrom().voidItem());
        lenient().when(awaitLiveCompletionRegistry.signal(any(), any()))
            .thenReturn(Uni.createFrom().item(false));
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
    void redriveExecutionQueuesTerminalDlqWithOriginalExecutionId() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        ExecutionRecord<Object, Object> terminal = recordWithStatus(
            createRecord(
                "tenant-1",
                "exec-redrive",
                "key-redrive",
                "pipeline-a",
                "contract-a",
                "release-a",
                ExecutionResultShape.SINGLE),
            ExecutionStatus.DLQ,
            4L,
            2);
        ExecutionRecord<Object, Object> redriven = recordWithStatus(terminal, ExecutionStatus.QUEUED, 5L, 3);

        when(executionStateStore.getExecution("tenant-1", "exec-redrive"))
            .thenReturn(Uni.createFrom().item(Optional.of(terminal)));
        when(executionStateStore.redriveTerminalExecution(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-redrive"),
                org.mockito.ArgumentMatchers.eq(4L),
                org.mockito.ArgumentMatchers.eq(false),
                org.mockito.ArgumentMatchers.startsWith("redrive:"),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(redriven)));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

        ExecutionRedriveResult result = coordinator.redriveExecution(
                "tenant-1",
                "exec-redrive",
                null,
                false,
                "operator checked downstream idempotency")
            .await().indefinitely();

        assertEquals("exec-redrive", result.executionId());
        assertEquals(ExecutionStatus.DLQ, result.previousStatus());
        assertEquals(ExecutionStatus.QUEUED, result.status());
        assertEquals("pipeline-a", result.pipelineId());
        assertEquals("contract-a", result.contractVersion());
        assertEquals("release-a", result.releaseVersion());
        assertEquals(3, result.attempt());
        verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-1", "exec-redrive"));
    }

    @Test
    void redriveExecutionRejectsFailedWithoutExplicitAllowFlag() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        ExecutionRecord<Object, Object> failed = recordWithStatus(
            createRecord("tenant-1", "exec-failed", "key-failed"),
            ExecutionStatus.FAILED,
            2L,
            1);
        when(executionStateStore.getExecution("tenant-1", "exec-failed"))
            .thenReturn(Uni.createFrom().item(Optional.of(failed)));

        assertThrows(IllegalStateException.class, () -> coordinator.redriveExecution(
                "tenant-1",
                "exec-failed",
                null,
                false,
                "operator retry")
            .await().indefinitely());

        verify(executionStateStore, never()).redriveTerminalExecution(
            any(), any(), org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyBoolean(), any(), org.mockito.ArgumentMatchers.anyLong());
        verify(workDispatcher, never()).enqueueNow(any());
    }

    @Test
    void redriveExecutionRejectsStaleExpectedVersion() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        ExecutionRecord<Object, Object> terminal = recordWithStatus(
            createRecord("tenant-1", "exec-stale", "key-stale"),
            ExecutionStatus.DLQ,
            7L,
            1);
        when(executionStateStore.getExecution("tenant-1", "exec-stale"))
            .thenReturn(Uni.createFrom().item(Optional.of(terminal)));

        assertThrows(IllegalStateException.class, () -> coordinator.redriveExecution(
                "tenant-1",
                "exec-stale",
                6L,
                false,
                "operator retry")
            .await().indefinitely());

        verify(executionStateStore, never()).redriveTerminalExecution(
            any(), any(), org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyBoolean(), any(), org.mockito.ArgumentMatchers.anyLong());
        verify(workDispatcher, never()).enqueueNow(any());
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
    void executePipelineAsyncRejectsDeniedTenantBeforeStoreAccess() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        PipelineOrchestratorConfig.TenancyConfig tenancy = mock(PipelineOrchestratorConfig.TenancyConfig.class);
        when(orchestratorConfig.tenancy()).thenReturn(tenancy);
        when(tenancy.allowedTenants()).thenReturn(List.of("tenant-allowed"));
        when(tenancy.requireExplicitTenant()).thenReturn(false);
        coordinator.controlPlaneAdmissionPolicy = new LocalControlPlaneAdmissionPolicy(orchestratorConfig);

        Uni<?> result = coordinator.executePipelineAsync("input", "tenant-denied", null, false);

        ControlPlaneAdmissionException error = assertThrows(
            ControlPlaneAdmissionException.class,
            () -> result.await().indefinitely());
        assertTrue(error.getMessage().contains("TENANT_NOT_ALLOWED"));
        verify(executionStateStore, never()).createOrGetExecution(any());
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
    void executePipelineAsyncScopesIdempotencyKeyByPipelineAndRelease() {
        configureQueueModeDefaults();
        PipelineReleaseIdentityResolver identityResolver = mock(PipelineReleaseIdentityResolver.class);
        coordinator.releaseIdentityResolver = identityResolver;
        when(identityResolver.pipelineId(orchestratorConfig)).thenReturn("pipeline-a", "pipeline-b");
        when(identityResolver.contractVersion()).thenReturn("contract-1", "contract-1");
        when(identityResolver.releaseVersion(orchestratorConfig)).thenReturn("release-1", "release-1");
        when(executionStateStore.createOrGetExecution(any()))
            .thenReturn(
                Uni.createFrom().item(new CreateExecutionResult(createRecord("tenant-1", "exec-a", "key-a"), false)),
                Uni.createFrom().item(new CreateExecutionResult(createRecord("tenant-1", "exec-b", "key-b"), false)));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

        coordinator.executePipelineAsync("input", "tenant-1", "client-key", false).await().indefinitely();
        coordinator.executePipelineAsync("input", "tenant-1", "client-key", false).await().indefinitely();

        ArgumentCaptor<org.pipelineframework.orchestrator.ExecutionCreateCommand> captor =
            ArgumentCaptor.forClass(org.pipelineframework.orchestrator.ExecutionCreateCommand.class);
        verify(executionStateStore, org.mockito.Mockito.times(2)).createOrGetExecution(captor.capture());
        List<org.pipelineframework.orchestrator.ExecutionCreateCommand> commands = captor.getAllValues();
        assertEquals("pipeline-a", commands.get(0).pipelineId());
        assertEquals("pipeline-b", commands.get(1).pipelineId());
        assertNotEquals(commands.get(0).executionKey(), commands.get(1).executionKey());
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
                command -> Uni.createFrom().item(TransitionResultEnvelope.waiting(new TransitionAwaitSuspension(
                    "tenant-1",
                    "exec-await",
                    "unit-1",
                    0))))
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
        when(awaitCoordinator.suspensionSnapshot(any(AwaitSuspendedException.class)))
            .thenReturn(Uni.createFrom().item(new TransitionAwaitSuspension("tenant-1", "exec-await", "unit-1", 2)));
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
                command -> Uni.createFrom().failure(new AwaitSuspendedException(
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
        when(awaitCoordinator.suspensionSnapshot(any(AwaitSuspendedException.class)))
            .thenReturn(Uni.createFrom().item(new TransitionAwaitSuspension("tenant-1", "exec-await", "unit-1", 0)));
        when(awaitCoordinator.getUnit("tenant-1", "unit-1"))
            .thenReturn(Uni.createFrom().item(awaitUnit("unit-1", AwaitUnitStatus.WAITING_EXTERNAL, 1, 0, false, null)));

        coordinator.processExecutionWorkItem(
                new ExecutionWorkItem("tenant-1", "exec-await"),
                command -> Uni.createFrom().failure(new IllegalStateException(
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
                command -> Uni.createFrom().item(TransitionResultEnvelope.waiting(new TransitionAwaitSuspension(
                    "tenant-1",
                    "exec-await",
                    "unit-1",
                    0))))
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
                command -> Uni.createFrom().item(TransitionResultEnvelope.completedInProcess(List.of("out-1", "out-2"))))
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
    void processExecutionWorkItemUsesStoredInputForAggregateAwaitItemContinuation() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.leaseMs()).thenReturn(1000L);
        List<String> aggregateInput = List.of("payment-output-1", "payment-output-2");
        ExecutionRecord<Object, Object> claimed = new ExecutionRecord<>(
            "tenant-1",
            "exec-aggregate",
            "key-aggregate",
            "local-pipeline",
            "local-bundle",
            ExecutionResultShape.MATERIALIZED_MULTI,
            ExecutionStatus.QUEUED,
            0L,
            7,
            0,
            null,
            0L,
            0L,
            null,
            aggregateInput,
            null,
            null,
            null,
            null,
            1L,
            1L,
            99999999L);
        when(executionStateStore.claimLease(any(), any(), any(), org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
        when(checkpointPublicationService.publishIfConfigured(org.mockito.ArgumentMatchers.eq(claimed), org.mockito.ArgumentMatchers.eq("output-file")))
            .thenReturn(Uni.createFrom().voidItem());
        when(executionStateStore.markSucceeded(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-aggregate"),
                org.mockito.ArgumentMatchers.eq(0L),
                org.mockito.ArgumentMatchers.eq("exec-aggregate:7:0"),
                org.mockito.ArgumentMatchers.eq(List.of("output-file")),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
        java.util.concurrent.atomic.AtomicReference<Object> workerInput = new java.util.concurrent.atomic.AtomicReference<>();

        coordinator.processExecutionWorkItem(
                new ExecutionWorkItem("tenant-1", "exec-aggregate"),
                envelope -> {
                    workerInput.set(envelope.toCommand(payloadCodec).inputPayload());
                    return Uni.createFrom().item(TransitionResultEnvelope.completedInProcess(List.of("output-file")));
                })
            .await().indefinitely();

        assertEquals(aggregateInput, workerInput.get());
        verify(awaitCoordinator, never()).loadResumePayload(any(), any());
        verify(executionStateStore).markSucceeded(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-aggregate"),
            org.mockito.ArgumentMatchers.eq(0L),
            org.mockito.ArgumentMatchers.eq("exec-aggregate:7:0"),
            org.mockito.ArgumentMatchers.eq(List.of("output-file")),
            org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    void processExecutionWorkItemPersistsDecodedInProcessOutputWithoutSerialization() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.leaseMs()).thenReturn(1000L);
        ExecutionRecord<Object, Object> claimed = createRecord("tenant-1", "exec-local", "key-local");
        NonSerializableOutput output = new NonSerializableOutput(new Object());
        when(executionStateStore.claimLease(any(), any(), any(), org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
        when(checkpointPublicationService.publishIfConfigured(org.mockito.ArgumentMatchers.eq(claimed), org.mockito.ArgumentMatchers.same(output)))
            .thenReturn(Uni.createFrom().voidItem());
        when(executionStateStore.markSucceeded(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-local"),
                org.mockito.ArgumentMatchers.eq(0L),
                org.mockito.ArgumentMatchers.eq("exec-local:0:0"),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(claimed)));

        coordinator.processExecutionWorkItem(
                new ExecutionWorkItem("tenant-1", "exec-local"),
                command -> Uni.createFrom().item(TransitionResultEnvelope.completedInProcess(List.of(output))))
            .await().indefinitely();

        ArgumentCaptor<Object> resultCaptor = ArgumentCaptor.forClass(Object.class);
        verify(executionStateStore).markSucceeded(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-local"),
            org.mockito.ArgumentMatchers.eq(0L),
            org.mockito.ArgumentMatchers.eq("exec-local:0:0"),
            resultCaptor.capture(),
            org.mockito.ArgumentMatchers.anyLong());
        List<?> persisted = assertInstanceOf(List.class, resultCaptor.getValue());
        assertEquals(output, persisted.getFirst());
    }

    @Test
    void processExecutionWorkItemPersistsRemoteSerializedOutputWithoutDecoding() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.leaseMs()).thenReturn(1000L);
        ExecutionRecord<Object, Object> claimed = createRecord("tenant-1", "exec-remote", "key-remote");
        SerializedTransitionPayload serialized = new SerializedTransitionPayload(
            "com.example.remote.NotOnCoordinatorClasspath",
            JsonTransitionPayloadCodec.ENCODING,
            "{\"value\":\"remote\"}");
        when(executionStateStore.claimLease(any(), any(), any(), org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
        when(checkpointPublicationService.publishIfConfigured(org.mockito.ArgumentMatchers.eq(claimed), org.mockito.ArgumentMatchers.eq(serialized)))
            .thenReturn(Uni.createFrom().voidItem());
        when(executionStateStore.markSucceeded(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-remote"),
                org.mockito.ArgumentMatchers.eq(0L),
                org.mockito.ArgumentMatchers.eq("exec-remote:0:0"),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(claimed)));

        coordinator.processExecutionWorkItem(
                new ExecutionWorkItem("tenant-1", "exec-remote"),
                command -> Uni.createFrom().item(new TransitionResultEnvelope(
                    TransitionWorkerOutcome.COMPLETED,
                    List.of(serialized),
                    null,
                    null)))
            .await().indefinitely();

        ArgumentCaptor<Object> resultCaptor = ArgumentCaptor.forClass(Object.class);
        verify(executionStateStore).markSucceeded(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-remote"),
            org.mockito.ArgumentMatchers.eq(0L),
            org.mockito.ArgumentMatchers.eq("exec-remote:0:0"),
            resultCaptor.capture(),
            org.mockito.ArgumentMatchers.anyLong());
        List<?> persisted = assertInstanceOf(List.class, resultCaptor.getValue());
        assertEquals(serialized, persisted.getFirst());
    }

    @Test
    void processExecutionWorkItemPublishesDecodedRemoteOutputWhenObjectPublishConfigured() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.leaseMs()).thenReturn(1000L);
        ExecutionRecord<Object, Object> claimed = createRecord("tenant-1", "exec-publish", "key-publish");
        SerializedTransitionPayload serialized = payloadCodec.encode("published-output");
        ObjectPublishCompletionService publishService = mock(ObjectPublishCompletionService.class);
        AtomicReference<List<?>> publishedItems = new AtomicReference<>();
        coordinator.objectPublishCompletionService = publishService;
        when(executionStateStore.claimLease(any(), any(), any(), org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(claimed)));
        when(checkpointPublicationService.publishIfConfigured(org.mockito.ArgumentMatchers.eq(claimed), org.mockito.ArgumentMatchers.eq(serialized)))
            .thenReturn(Uni.createFrom().voidItem());
        when(publishService.publishIfConfigured(org.mockito.ArgumentMatchers.<Supplier<List<?>>>any()))
            .thenAnswer(invocation -> {
                Supplier<List<?>> supplier = invocation.getArgument(0);
                publishedItems.set(supplier.get());
                return Uni.createFrom().voidItem();
            });
        when(executionStateStore.markSucceeded(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-publish"),
                org.mockito.ArgumentMatchers.eq(0L),
                org.mockito.ArgumentMatchers.eq("exec-publish:0:0"),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(claimed)));

        coordinator.processExecutionWorkItem(
                new ExecutionWorkItem("tenant-1", "exec-publish"),
                command -> Uni.createFrom().item(new TransitionResultEnvelope(
                    TransitionWorkerOutcome.COMPLETED,
                    List.of(serialized),
                    null,
                    null)))
            .await().indefinitely();

        assertEquals(List.of("published-output"), publishedItems.get());
        ArgumentCaptor<Object> resultCaptor = ArgumentCaptor.forClass(Object.class);
        verify(executionStateStore).markSucceeded(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-publish"),
            org.mockito.ArgumentMatchers.eq(0L),
            org.mockito.ArgumentMatchers.eq("exec-publish:0:0"),
            resultCaptor.capture(),
            org.mockito.ArgumentMatchers.anyLong());
        List<?> persisted = assertInstanceOf(List.class, resultCaptor.getValue());
        assertEquals(serialized, persisted.getFirst());
    }

    @Test
    void transitionCommandUsesExecutionPinnedReleaseIdentity() {
        TransitionCommandEnvelope envelope = prepareTransitionCommand(
            createRecord(
                "tenant-1",
                "exec-bundle",
                "key-bundle",
                "org.example.pinned",
                "sha256:contract",
                "sha256:release",
                ExecutionResultShape.SINGLE),
            "trace-1");

        assertEquals("org.example.pinned", envelope.pipelineId());
        assertEquals("sha256:contract", envelope.contractVersion());
        assertEquals("sha256:release", envelope.releaseVersion());
        assertEquals("trace-1", envelope.traceId());
    }

    @Test
    void processExecutionWorkItemDoesNotClaimLeaseWhenAdmissionIsSaturated() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(workerConfig.maxInFlight()).thenReturn(1);
        when(workDispatcher.enqueueDelayed(any(), org.mockito.ArgumentMatchers.eq(Duration.ofMillis(25))))
            .thenReturn(Uni.createFrom().voidItem());
        when(workerConfig.saturatedDelay()).thenReturn(Duration.ofMillis(25));
        TransitionWorkerExecutor executor = new TransitionWorkerExecutor(
            orchestratorConfig,
            new PipelineInvocationRuntime());
        coordinator.transitionWorkerExecutor = executor;
        TransitionWorkerExecutor.TransitionAdmission heldPermit = executor.tryAdmit().orElseThrow();

        try {
            coordinator.processExecutionWorkItem(
                    new ExecutionWorkItem("tenant-1", "exec-saturated"),
                    command -> Uni.createFrom().item(TransitionResultEnvelope.completed(payloadCodec, List.of())))
                .await().indefinitely();
        } finally {
            heldPermit.close();
        }

        verify(executionStateStore, never()).claimLease(
            any(),
            any(),
            any(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyLong());
        verify(workDispatcher).enqueueDelayed(
            org.mockito.ArgumentMatchers.eq(new ExecutionWorkItem("tenant-1", "exec-saturated")),
            org.mockito.ArgumentMatchers.eq(Duration.ofMillis(25)));
    }

    @Test
    void processExecutionWorkItemDoesNotClaimLeaseWhenTenantQuotaIsSaturated() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(workerConfig.maxInFlight()).thenReturn(64);
        when(workerConfig.saturatedDelay()).thenReturn(Duration.ofMillis(25));
        when(workDispatcher.enqueueDelayed(any(), org.mockito.ArgumentMatchers.eq(Duration.ofMillis(25))))
            .thenReturn(Uni.createFrom().voidItem());
        PipelineOrchestratorConfig.TenancyConfig tenancy = mock(PipelineOrchestratorConfig.TenancyConfig.class);
        PipelineOrchestratorConfig.QuotaConfig quotas = mock(PipelineOrchestratorConfig.QuotaConfig.class);
        when(orchestratorConfig.tenancy()).thenReturn(tenancy);
        when(orchestratorConfig.quotas()).thenReturn(quotas);
        when(tenancy.allowedTenants()).thenReturn(List.of());
        when(tenancy.requireExplicitTenant()).thenReturn(false);
        when(quotas.maxInFlightTransitionsPerTenant()).thenReturn(1);
        LocalControlPlaneAdmissionPolicy policy = new LocalControlPlaneAdmissionPolicy(orchestratorConfig);
        coordinator.controlPlaneAdmissionPolicy = policy;
        ControlPlaneTransitionAdmission held = policy.admitTransition(new ControlPlaneAdmissionRequest(
            "tenant-1",
            ControlPlaneAdmissionOperation.PROCESS_WORK_ITEM,
            "local-pipeline",
            "local-bundle",
            "exec-held",
            "test",
            true));

        try {
            coordinator.processExecutionWorkItem(
                    new ExecutionWorkItem("tenant-1", "exec-saturated"),
                    command -> Uni.createFrom().item(TransitionResultEnvelope.completed(payloadCodec, List.of())))
                .await().indefinitely();
        } finally {
            held.permit().close();
        }

        verify(executionStateStore, never()).claimLease(
            any(),
            any(),
            any(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyLong());
        verify(workDispatcher).enqueueDelayed(
            org.mockito.ArgumentMatchers.eq(new ExecutionWorkItem("tenant-1", "exec-saturated")),
            org.mockito.ArgumentMatchers.eq(Duration.ofMillis(25)));
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
    void completeAwaitDoesNotDispatchItemContinuationUntilParentIsWaitingExternal() {
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
        ExecutionRecord<Object, Object> runningParent = createRecord("tenant-1", "exec-1", "key-1");
        AwaitItemContinuationHandler handler = mock(AwaitItemContinuationHandler.class);
        when(awaitCoordinator.complete(command))
            .thenReturn(Uni.createFrom().item(new AwaitCompletionResult(completed, false)));
        when(awaitCoordinator.recordCompletion(org.mockito.ArgumentMatchers.eq(completed), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(awaitUnit("unit-1", AwaitUnitStatus.COMPLETED, 1, 1, true, null)));
        when(executionStateStore.getExecution("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(Optional.of(runningParent)));

        AwaitCompletionResult result = coordinator.completeAwait(command, handler).await().indefinitely();

        assertEquals(completed.interactionId(), result.record().interactionId());
        verify(handler, never()).continueAwaitItem(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyInt(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyLong());
        verify(workDispatcher, never()).enqueueNow(any());
    }

    @Test
    void completeAwaitUsesLiveAwaitStreamInsteadOfDurableItemContinuationWhenAccepted() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        AwaitInteractionRecord completed = itemAwaitRecord(0, AwaitInteractionStatus.COMPLETED, "approved");
        AwaitUnitRecord unit = awaitUnit("unit-1", AwaitUnitStatus.COMPLETED, 1, 1, true, null);
        AwaitCompletionCommand command = new AwaitCompletionCommand(
            "tenant-1",
            completed.interactionId(),
            null,
            null,
            java.util.Map.of("value", "approved"),
            "user-1",
            System.currentTimeMillis());
        AwaitItemContinuationHandler handler = mock(AwaitItemContinuationHandler.class);
        when(awaitCoordinator.complete(command))
            .thenReturn(Uni.createFrom().item(new AwaitCompletionResult(completed, false)));
        when(awaitCoordinator.recordCompletion(org.mockito.ArgumentMatchers.eq(completed), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(unit));
        when(awaitLiveCompletionRegistry.signal(completed, unit))
            .thenReturn(Uni.createFrom().item(true));

        AwaitCompletionResult result = coordinator.completeAwait(command, handler).await().indefinitely();

        assertEquals(completed.interactionId(), result.record().interactionId());
        verify(awaitLiveCompletionRegistry).signal(completed, unit);
        verify(handler, never()).continueAwaitItem(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyInt(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyLong());
        verify(handler, never()).releaseAwaitParentIfReady(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyInt(),
            org.mockito.ArgumentMatchers.anyLong());
        verify(executionStateStore, never()).getExecution(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any());
    }

    @Test
    void completeAwaitFallsBackToDurableContinuationWhenLiveAwaitSignalFails() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        AwaitInteractionRecord completed = itemAwaitRecord(0, AwaitInteractionStatus.COMPLETED, "approved");
        AwaitUnitRecord unit = awaitUnit("unit-1", AwaitUnitStatus.COMPLETED, 1, 1, true, null);
        AwaitCompletionCommand command = new AwaitCompletionCommand(
            "tenant-1",
            completed.interactionId(),
            null,
            null,
            java.util.Map.of("value", "approved"),
            "user-1",
            System.currentTimeMillis());
        ExecutionRecord<Object, Object> runningParent = createRecord("tenant-1", "exec-1", "key-1");
        AwaitItemContinuationHandler handler = mock(AwaitItemContinuationHandler.class);
        when(awaitCoordinator.complete(command))
            .thenReturn(Uni.createFrom().item(new AwaitCompletionResult(completed, false)));
        when(awaitCoordinator.recordCompletion(org.mockito.ArgumentMatchers.eq(completed), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(unit));
        when(awaitLiveCompletionRegistry.signal(completed, unit))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("closed live session")));
        when(executionStateStore.getExecution("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(Optional.of(runningParent)));

        AwaitCompletionResult result = coordinator.completeAwait(command, handler).await().indefinitely();

        assertEquals(completed.interactionId(), result.record().interactionId());
        verify(awaitLiveCompletionRegistry).signal(completed, unit);
        verify(executionStateStore).getExecution("tenant-1", "exec-1");
        verify(handler, never()).continueAwaitItem(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyInt(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    void awaitItemContinuationDispatchAttemptRechecksParentWaitingExternalState() {
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
        ExecutionRecord<Object, Object> waitingParent = new ExecutionRecord<>(
            "tenant-1",
            "exec-1",
            "key-1",
            ExecutionResultShape.MATERIALIZED_MULTI,
            ExecutionStatus.WAITING_EXTERNAL,
            1L,
            2,
            0,
            null,
            0L,
            Long.MAX_VALUE,
            "exec-1:0:0",
            "input",
            "unit-1",
            null,
            null,
            null,
            1L,
            2L,
            99999999L);
        ExecutionRecord<Object, Object> runningParent = createRecord("tenant-1", "exec-1", "key-1");
        AwaitItemContinuationHandler handler = mock(AwaitItemContinuationHandler.class);
        when(awaitCoordinator.complete(command))
            .thenReturn(Uni.createFrom().item(new AwaitCompletionResult(completed, false)));
        when(awaitCoordinator.recordCompletion(org.mockito.ArgumentMatchers.eq(completed), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(awaitUnit("unit-1", AwaitUnitStatus.COMPLETED, 1, 1, true, null)));
        when(executionStateStore.getExecution("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(Optional.of(waitingParent)))
            .thenReturn(Uni.createFrom().item(Optional.of(runningParent)));

        AwaitCompletionResult result = coordinator.completeAwait(command, handler).await().indefinitely();

        assertEquals(completed.interactionId(), result.record().interactionId());
        verify(handler, after(300).never()).continueAwaitItem(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyInt(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    void processExecutionWorkItemDispatchesEarlyCompletedItemContinuationsAfterParentIsWaitingExternal() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.leaseMs()).thenReturn(1000L);
        ExecutionRecord<Object, Object> claimed = createRecord("tenant-1", "exec-await", "key-await");
        ExecutionRecord<Object, Object> waiting = new ExecutionRecord<>(
            "tenant-1",
            "exec-await",
            "key-await",
            ExecutionResultShape.MATERIALIZED_MULTI,
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
        AwaitUnitRecord completedUnit = awaitUnit("unit-1", AwaitUnitStatus.COMPLETED, 2, 2, true, null);
        AwaitItemContinuationHandler handler = mock(AwaitItemContinuationHandler.class);
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
            .thenReturn(Uni.createFrom().item(completedUnit));
        when(awaitCoordinator.suspensionSnapshot(any(AwaitSuspendedException.class)))
            .thenReturn(Uni.createFrom().item(new TransitionAwaitSuspension("tenant-1", "exec-await", "unit-1", 2)));
        when(awaitCoordinator.findByUnit("tenant-1", "unit-1"))
            .thenReturn(Uni.createFrom().item(List.of(
                itemAwaitRecord("exec-await", 0, AwaitInteractionStatus.COMPLETED, "approved-0"),
                itemAwaitRecord("exec-await", 1, AwaitInteractionStatus.COMPLETED, "approved-1"))));
        when(executionStateStore.getExecution("tenant-1", "exec-await"))
            .thenReturn(Uni.createFrom().item(Optional.of(waiting)));
        when(handler.continueAwaitItem(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.eq(completedUnit),
                org.mockito.ArgumentMatchers.eq(3),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().voidItem());
        when(handler.releaseAwaitParentIfReady(
                org.mockito.ArgumentMatchers.eq(waiting),
                org.mockito.ArgumentMatchers.eq(completedUnit),
                org.mockito.ArgumentMatchers.eq(3),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().voidItem());

        coordinator.processExecutionWorkItem(
                new ExecutionWorkItem("tenant-1", "exec-await"),
                command -> Uni.createFrom().failure(new AwaitSuspendedException(
                    "tenant-1",
                    "exec-await",
                    "unit-1",
                    2)),
                handler)
            .await().indefinitely();

        verify(handler, timeout(1000).times(2)).continueAwaitItem(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.eq(completedUnit),
            org.mockito.ArgumentMatchers.eq(3),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyLong());
        verify(handler, timeout(1000)).releaseAwaitParentIfReady(
            org.mockito.ArgumentMatchers.eq(waiting),
            org.mockito.ArgumentMatchers.eq(completedUnit),
            org.mockito.ArgumentMatchers.eq(3),
            org.mockito.ArgumentMatchers.anyLong());
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

    @Test
    void timeoutSweepStopsWhenFailureWriteLosesRaceToTerminalExecution() {
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
        ExecutionRecord<Object, Object> succeeded = recordWithStatus(waiting, ExecutionStatus.SUCCEEDED, 8L, 0);
        when(awaitCoordinator.findTimedOut(org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.eq(100)))
            .thenReturn(Uni.createFrom().item(java.util.List.of(interaction)));
        when(awaitCoordinator.markTimedOut(org.mockito.ArgumentMatchers.eq(interaction), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(interaction)));
        when(executionStateStore.getExecution("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(Optional.of(waiting)))
            .thenReturn(Uni.createFrom().item(Optional.of(succeeded)));
        when(executionStateStore.markTerminalFailure(
                org.mockito.ArgumentMatchers.eq("tenant-1"),
                org.mockito.ArgumentMatchers.eq("exec-1"),
                org.mockito.ArgumentMatchers.eq(7L),
                org.mockito.ArgumentMatchers.eq(ExecutionStatus.FAILED),
                org.mockito.ArgumentMatchers.eq("exec-1:2:0"),
                org.mockito.ArgumentMatchers.eq("AWAIT_TIMEOUT"),
                org.mockito.ArgumentMatchers.contains("interaction-1"),
                org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.empty()));
        when(executionStateStore.findDueExecutions(org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.eq(100)))
            .thenReturn(Uni.createFrom().item(java.util.List.of()));

        coordinator.sweepDueExecutions();

        verify(executionStateStore, timeout(500).times(1)).markTerminalFailure(
            org.mockito.ArgumentMatchers.eq("tenant-1"),
            org.mockito.ArgumentMatchers.eq("exec-1"),
            org.mockito.ArgumentMatchers.eq(7L),
            org.mockito.ArgumentMatchers.eq(ExecutionStatus.FAILED),
            org.mockito.ArgumentMatchers.eq("exec-1:2:0"),
            org.mockito.ArgumentMatchers.eq("AWAIT_TIMEOUT"),
            org.mockito.ArgumentMatchers.contains("interaction-1"),
            org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    void timeoutSweepDoesNotFailParentWhenInteractionTimeoutMarkLosesRace() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.sweepLimit()).thenReturn(100);
        AwaitInteractionRecord interaction = awaitRecord();
        when(awaitCoordinator.findTimedOut(org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.eq(100)))
            .thenReturn(Uni.createFrom().item(java.util.List.of(interaction)));
        when(awaitCoordinator.markTimedOut(org.mockito.ArgumentMatchers.eq(interaction), org.mockito.ArgumentMatchers.anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.empty()));
        when(executionStateStore.findDueExecutions(org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.eq(100)))
            .thenReturn(Uni.createFrom().item(java.util.List.of()));

        coordinator.sweepDueExecutions();

        verify(executionStateStore, after(200).never()).markTerminalFailure(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(),
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

    private ExecutionRecord<Object, Object> recordWithStatus(
        ExecutionRecord<Object, Object> source,
        ExecutionStatus status,
        long version,
        int attempt) {
        return new ExecutionRecord<>(
            source.tenantId(),
            source.executionId(),
            source.executionKey(),
            source.pipelineId(),
            source.contractVersion(),
            source.releaseVersion(),
            source.resultShape(),
            status,
            version,
            source.currentStepIndex(),
            attempt,
            source.leaseOwner(),
            source.leaseExpiresEpochMs(),
            source.nextDueEpochMs(),
            source.lastTransitionKey(),
            source.inputPayload(),
            source.awaitUnitId(),
            source.resultPayload(),
            source.errorCode(),
            source.errorMessage(),
            source.createdAtEpochMs(),
            source.updatedAtEpochMs(),
            source.ttlEpochS());
    }

    private ExecutionRecord<Object, Object> createRecord(
        String tenantId,
        String executionId,
        String executionKey,
        ExecutionResultShape resultShape) {
        return createRecord(
            tenantId,
            executionId,
            executionKey,
            "local-pipeline",
            "local-contract",
            "local-contract",
            resultShape);
    }

    private ExecutionRecord<Object, Object> createRecord(
        String tenantId,
        String executionId,
        String executionKey,
        String pipelineId,
        String contractVersion,
        String releaseVersion,
        ExecutionResultShape resultShape) {
        return new ExecutionRecord<>(
            tenantId,
            executionId,
            executionKey,
            pipelineId,
            contractVersion,
            releaseVersion,
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

    private record NonSerializableOutput(Object writer) {
    }

    @SuppressWarnings("unchecked")
    private List<?> runAsyncExecution(
        ExecutionRecord<Object, Object> record,
        Function<ExecutionRecord<Object, Object>, Multi<?>> executeStreaming) {
        try {
            Method method = QueueAsyncCoordinator.class.getDeclaredMethod(
                "runAsyncExecution",
                ExecutionRecord.class,
                org.pipelineframework.orchestrator.PipelineTransitionWorker.class);
            method.setAccessible(true);
            return ((Uni<List<?>>) method.invoke(
                coordinator,
                record,
                (org.pipelineframework.orchestrator.PipelineTransitionWorker) command ->
                    executeStreaming.apply(new ExecutionRecord<>(
                            command.tenantId(),
                            command.executionId(),
                            "key-" + command.executionId(),
                            command.resultShape(),
                            ExecutionStatus.RUNNING,
                            command.executionVersion(),
                            command.currentStepIndex(),
                            command.attempt(),
                            null,
                            0L,
                            0L,
                            null,
                            command.toCommand(payloadCodec).inputPayload(),
                            null,
                            null,
                            null,
                            null,
                            1L,
                            1L,
                            99999999L))
                        .collect().asList()
                        .onItem().transform(TransitionResultEnvelope::completedInProcess)))
                .await().indefinitely();
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Failed invoking runAsyncExecution", e);
        }
    }

    @SuppressWarnings("unchecked")
    private TransitionCommandEnvelope prepareTransitionCommand(
        ExecutionRecord<Object, Object> record,
        String transitionKey) {
        try {
            Method method = QueueAsyncCoordinator.class.getDeclaredMethod(
                "prepareTransitionCommand",
                ExecutionRecord.class,
                String.class);
            method.setAccessible(true);
            return ((Uni<TransitionCommandEnvelope>) method.invoke(coordinator, record, transitionKey))
                .await().indefinitely();
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Failed invoking prepareTransitionCommand", e);
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
            java.util.Set.of(),
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
