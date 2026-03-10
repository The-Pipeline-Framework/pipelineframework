package org.pipelineframework;

import java.time.Duration;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
        when(executionStateStore.findDueExecutions(org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyInt()))
            .thenReturn(Uni.createFrom().item(java.util.List.of(
                createRecord("tenant-a", "exec-5", "key-5"),
                createRecord("tenant-b", "exec-6", "key-6"))));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

        coordinator.sweepDueExecutions();

        ArgumentCaptor<ExecutionWorkItem> itemCaptor = ArgumentCaptor.forClass(ExecutionWorkItem.class);
        verify(workDispatcher, timeout(500).times(2)).enqueueNow(itemCaptor.capture());
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
}
