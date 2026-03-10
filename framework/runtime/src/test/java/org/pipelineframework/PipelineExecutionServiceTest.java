package org.pipelineframework;

import java.util.Optional;

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
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.EventWorkDispatcher;
import org.pipelineframework.orchestrator.InMemoryExecutionStateStore;
import org.pipelineframework.orchestrator.LoggingDeadLetterPublisher;
import org.pipelineframework.orchestrator.OrchestratorIdempotencyPolicy;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.SqsDeadLetterPublisher;
import org.pipelineframework.orchestrator.SqsWorkDispatcher;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.DynamoExecutionStateStore;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PipelineExecutionServiceTest {

    private PipelineExecutionService service;

    @Mock
    private PipelineOrchestratorConfig orchestratorConfig;

    @Mock
    private ExecutionStateStore executionStateStore;

    @Mock
    private WorkDispatcher workDispatcher;

    @Mock
    private Instance<ExecutionStateStore> executionStateStores;

    @Mock
    private Instance<WorkDispatcher> workDispatchers;

    @Mock
    private Instance<DeadLetterPublisher> deadLetterPublishers;

    @BeforeEach
    void setUp() throws Exception {
        service = new PipelineExecutionService();
        setField("orchestratorConfig", orchestratorConfig);
    }

    @Test
    void startupHealthStateInitiallyPending() {
        assertEquals(PipelineExecutionService.StartupHealthState.PENDING, service.getStartupHealthState());
    }

    @Test
    void executePipelineAsyncRequiresQueueMode() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.SYNC);

        Uni<?> result = service.executePipelineAsync("input", "tenant-1", "idem-1");

        assertThrows(IllegalStateException.class, () -> result.await().indefinitely());
    }

    @Test
    void executePipelineAsyncRejectsStreamingOutputFlagInQueueMode() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);

        Uni<?> result = service.executePipelineAsync("input", "tenant-1", "idem-1", true);

        IllegalStateException error = assertThrows(IllegalStateException.class, () -> result.await().indefinitely());
        assertTrue(error.getMessage().contains("does not support streaming pipeline outputs"));
    }

    @Test
    void initializeQueueModeFailsFastWhenSelectedProviderReportsStartupError() throws Exception {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.stateProvider()).thenReturn("dynamo");
        when(orchestratorConfig.dispatcherProvider()).thenReturn("event");
        when(orchestratorConfig.dlqProvider()).thenReturn("log");
        when(orchestratorConfig.strictStartup()).thenReturn(true);

        when(executionStateStores.stream()).thenReturn(java.util.stream.Stream.of(new DynamoExecutionStateStore()));
        when(workDispatchers.stream()).thenReturn(java.util.stream.Stream.of(new EventWorkDispatcher()));
        when(deadLetterPublishers.stream()).thenReturn(java.util.stream.Stream.of(new LoggingDeadLetterPublisher()));
        setField("executionStateStores", executionStateStores);
        setField("workDispatchers", workDispatchers);
        setField("deadLetterPublishers", deadLetterPublishers);

        IllegalStateException error = assertThrows(
            IllegalStateException.class,
            this::invokeInitializeQueueMode);
        assertTrue(error.getMessage().contains("ExecutionStateStore(dynamo)"));
    }

    @Test
    void initializeQueueModeFailsFastWhenStrictStartupAndIdempotencyPolicyIsNotExplicit() throws Exception {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.stateProvider()).thenReturn("memory");
        when(orchestratorConfig.dispatcherProvider()).thenReturn("event");
        when(orchestratorConfig.dlqProvider()).thenReturn("log");
        when(orchestratorConfig.strictStartup()).thenReturn(true);
        when(orchestratorConfig.idempotencyPolicy()).thenReturn(OrchestratorIdempotencyPolicy.OPTIONAL_CLIENT_KEY);

        when(executionStateStores.stream()).thenReturn(java.util.stream.Stream.of(new InMemoryExecutionStateStore()));
        when(workDispatchers.stream()).thenReturn(java.util.stream.Stream.of(new EventWorkDispatcher()));
        when(deadLetterPublishers.stream()).thenReturn(java.util.stream.Stream.of(new LoggingDeadLetterPublisher()));
        setField("executionStateStores", executionStateStores);
        setField("workDispatchers", workDispatchers);
        setField("deadLetterPublishers", deadLetterPublishers);

        IllegalStateException error = assertThrows(
            IllegalStateException.class,
            this::invokeInitializeQueueMode);
        assertTrue(error.getMessage().contains("idempotency-policy must be explicitly configured"));
    }

    @Test
    void initializeQueueModeAggregatesProviderDiagnosticsWhenStrictStartupIsEnabled() throws Exception {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.stateProvider()).thenReturn("dynamo");
        when(orchestratorConfig.dispatcherProvider()).thenReturn("sqs");
        when(orchestratorConfig.dlqProvider()).thenReturn("sqs");
        when(orchestratorConfig.strictStartup()).thenReturn(true);

        when(executionStateStores.stream()).thenReturn(java.util.stream.Stream.of(new DynamoExecutionStateStore()));
        when(workDispatchers.stream()).thenReturn(java.util.stream.Stream.of(new SqsWorkDispatcher()));
        when(deadLetterPublishers.stream()).thenReturn(java.util.stream.Stream.of(new SqsDeadLetterPublisher()));
        setField("executionStateStores", executionStateStores);
        setField("workDispatchers", workDispatchers);
        setField("deadLetterPublishers", deadLetterPublishers);

        IllegalStateException error = assertThrows(
            IllegalStateException.class,
            this::invokeInitializeQueueMode);
        assertTrue(error.getMessage().contains("ExecutionStateStore(dynamo)"));
        assertTrue(error.getMessage().contains("WorkDispatcher(sqs)"));
        assertTrue(error.getMessage().contains("DeadLetterPublisher(sqs)"));
    }

    @Test
    void getExecutionStatusReturnsRecordStateInQueueMode() throws Exception {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        setField("executionStateStore", executionStateStore);
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

        ExecutionStatusDto dto = service.getExecutionStatus("tenant-1", "exec-1").await().indefinitely();

        assertNotNull(dto);
        assertEquals("exec-1", dto.executionId());
        assertEquals(ExecutionStatus.RUNNING, dto.status());
        verify(executionStateStore).getExecution("tenant-1", "exec-1");
    }

    @Test
    void executePipelineAsyncPersistsUniInputShapeForPlainArrayPayload() throws Exception {
        configureQueueModeDefaults();
        setField("executionStateStore", executionStateStore);
        setField("workDispatcher", workDispatcher);
        when(executionStateStore.createOrGetExecution(any()))
            .thenReturn(Uni.createFrom().item(new CreateExecutionResult(
                createRecord("tenant-1", "exec-uni", "key-uni"),
                true)));

        service.executePipelineAsync(java.util.List.of("a", "b"), "tenant-1", null).await().indefinitely();

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
    void executePipelineAsyncPersistsMultiInputShapeForStreamingPayload() throws Exception {
        configureQueueModeDefaults();
        setField("executionStateStore", executionStateStore);
        setField("workDispatcher", workDispatcher);
        when(executionStateStore.createOrGetExecution(any()))
            .thenReturn(Uni.createFrom().item(new CreateExecutionResult(
                createRecord("tenant-1", "exec-multi", "key-multi"),
                true)));

        service.executePipelineAsync(Multi.createFrom().items("x", "y"), "tenant-1", null).await().indefinitely();

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
    void executePipelineAsyncReturnsDeterministicDuplicateResponseAndSkipsSecondEnqueue() throws Exception {
        configureQueueModeDefaults();
        setField("executionStateStore", executionStateStore);
        setField("workDispatcher", workDispatcher);
        ExecutionRecord<Object, Object> record = createRecord("tenant-1", "exec-duplicate", "key-duplicate");

        when(executionStateStore.createOrGetExecution(any()))
            .thenReturn(Uni.createFrom().item(new CreateExecutionResult(record, false)))
            .thenReturn(Uni.createFrom().item(new CreateExecutionResult(record, true)));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

        RunAsyncAcceptedDto first = service.executePipelineAsync("input", "tenant-1", "idem-dup")
            .await().indefinitely();
        RunAsyncAcceptedDto second = service.executePipelineAsync("input", "tenant-1", "idem-dup")
            .await().indefinitely();

        assertFalse(first.duplicate());
        assertTrue(second.duplicate());
        assertEquals(first.executionId(), second.executionId());
        verify(workDispatcher, times(1)).enqueueNow(any());
    }

    @Test
    void executePipelineAsyncWithServerKeyOnlyDeduplicatesWithoutClientKey() throws Exception {
        configureQueueModeDefaults();
        when(orchestratorConfig.idempotencyPolicy()).thenReturn(OrchestratorIdempotencyPolicy.SERVER_KEY_ONLY);
        setField("executionStateStore", executionStateStore);
        setField("workDispatcher", workDispatcher);
        ExecutionRecord<Object, Object> record = createRecord("tenant-1", "exec-server-key", "key-server-key");

        when(executionStateStore.createOrGetExecution(any()))
            .thenReturn(Uni.createFrom().item(new CreateExecutionResult(record, false)))
            .thenReturn(Uni.createFrom().item(new CreateExecutionResult(record, true)));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

        RunAsyncAcceptedDto first = service.executePipelineAsync("input", "tenant-1", null)
            .await().indefinitely();
        RunAsyncAcceptedDto second = service.executePipelineAsync("input", "tenant-1", null)
            .await().indefinitely();

        assertFalse(first.duplicate());
        assertTrue(second.duplicate());
        assertEquals(first.executionId(), second.executionId());
        verify(workDispatcher, times(1)).enqueueNow(any());
    }

    @Test
    void getExecutionResultReturnsUnaryPayloadWhenSucceeded() throws Exception {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        setField("executionStateStore", executionStateStore);
        ExecutionRecord<Object, Object> record = new ExecutionRecord<>(
            "tenant-1",
            "exec-succeeded",
            "key-succeeded",
            ExecutionStatus.SUCCEEDED,
            2L,
            0,
            0,
            null,
            0L,
            0L,
            null,
            "input",
            java.util.List.of("payload-ok"),
            null,
            null,
            1L,
            2L,
            99999999L);
        when(executionStateStore.getExecution("tenant-1", "exec-succeeded"))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));

        String payload = service.<String>getExecutionResult("tenant-1", "exec-succeeded", String.class, false)
            .await().indefinitely();

        assertEquals("payload-ok", payload);
    }

    @Test
    void getExecutionResultFailsWhenExecutionIsNotComplete() throws Exception {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        setField("executionStateStore", executionStateStore);
        ExecutionRecord<Object, Object> record = new ExecutionRecord<>(
            "tenant-1",
            "exec-running",
            "key-running",
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
        when(executionStateStore.getExecution("tenant-1", "exec-running"))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));

        IllegalStateException error = assertThrows(IllegalStateException.class, () ->
            service.getExecutionResult("tenant-1", "exec-running", String.class, false)
                .await().indefinitely());
        assertTrue(error.getMessage().contains("not complete yet"));
    }

    @Test
    void getExecutionResultFailsWhenExecutionReachedTerminalFailure() throws Exception {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        setField("executionStateStore", executionStateStore);
        ExecutionRecord<Object, Object> record = new ExecutionRecord<>(
            "tenant-1",
            "exec-failed",
            "key-failed",
            ExecutionStatus.FAILED,
            2L,
            0,
            1,
            null,
            0L,
            0L,
            "exec-failed:0:1",
            "input",
            null,
            "IllegalStateException",
            "failure",
            1L,
            2L,
            99999999L);
        when(executionStateStore.getExecution("tenant-1", "exec-failed"))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));

        IllegalStateException error = assertThrows(IllegalStateException.class, () ->
            service.getExecutionResult("tenant-1", "exec-failed", String.class, false)
                .await().indefinitely());
        assertTrue(error.getMessage().contains("finished without a successful result"));
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

    private void setField(String fieldName, Object value) throws Exception {
        var field = PipelineExecutionService.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(service, value);
    }

    private void invokeInitializeQueueMode() throws Exception {
        var method = PipelineExecutionService.class.getDeclaredMethod("initializeQueueMode");
        method.setAccessible(true);
        try {
            method.invoke(service);
        } catch (java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof Exception exception) {
                throw exception;
            }
            throw e;
        }
    }
}
