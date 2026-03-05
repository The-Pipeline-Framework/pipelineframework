package org.pipelineframework;

import java.util.Optional;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
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
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.OrchestratorIdempotencyPolicy;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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

    @Test
    void getExecutionResultReturnsSuccessfulPayloadInQueueMode() throws Exception {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        setField("executionStateStore", executionStateStore);
        ExecutionRecord<Object, Object> record = new ExecutionRecord<>(
            "tenant-1",
            "exec-1",
            "key-1",
            ExecutionStatus.SUCCEEDED,
            1L,
            0,
            0,
            null,
            0L,
            0L,
            null,
            "input",
            java.util.List.of("result1"),
            null,
            null,
            1L,
            1L,
            99999999L);
        when(executionStateStore.getExecution("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));

        Object result = service.getExecutionResult("tenant-1", "exec-1", String.class, false)
            .await().indefinitely();

        assertNotNull(result);
        assertEquals("result1", result);
        verify(executionStateStore).getExecution("tenant-1", "exec-1");
    }

    @Test
    void getExecutionResultThrowsWhenExecutionNotFound() throws Exception {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        setField("executionStateStore", executionStateStore);
        when(executionStateStore.getExecution("tenant-1", "missing"))
            .thenReturn(Uni.createFrom().item(Optional.empty()));

        assertThrows(jakarta.ws.rs.NotFoundException.class,
            () -> service.getExecutionResult("tenant-1", "missing", String.class, false)
                .await().indefinitely());
    }

    @Test
    void getExecutionResultThrowsWhenExecutionNotComplete() throws Exception {
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

        assertThrows(IllegalStateException.class,
            () -> service.getExecutionResult("tenant-1", "exec-1", String.class, false)
                .await().indefinitely());
    }

    @Test
    void getExecutionResultThrowsWhenExecutionFailed() throws Exception {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        setField("executionStateStore", executionStateStore);
        ExecutionRecord<Object, Object> record = new ExecutionRecord<>(
            "tenant-1",
            "exec-1",
            "key-1",
            ExecutionStatus.FAILED,
            1L,
            0,
            0,
            null,
            0L,
            0L,
            null,
            "input",
            null,
            "ErrorCode",
            "Error message",
            1L,
            1L,
            99999999L);
        when(executionStateStore.getExecution("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> service.getExecutionResult("tenant-1", "exec-1", String.class, false)
                .await().indefinitely());

        assertTrue(exception.getMessage().contains("finished without a successful result"));
    }

    @Test
    void getExecutionResultHandlesStreamingOutputMode() throws Exception {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        setField("executionStateStore", executionStateStore);
        java.util.List<String> resultList = java.util.List.of("item1", "item2", "item3");
        ExecutionRecord<Object, Object> record = new ExecutionRecord<>(
            "tenant-1",
            "exec-1",
            "key-1",
            ExecutionStatus.SUCCEEDED,
            1L,
            0,
            0,
            null,
            0L,
            0L,
            null,
            "input",
            resultList,
            null,
            null,
            1L,
            1L,
            99999999L);
        when(executionStateStore.getExecution("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));

        Object result = service.getExecutionResult("tenant-1", "exec-1", String.class, true)
            .await().indefinitely();

        assertNotNull(result);
        assertEquals(resultList, result);
    }

    @Test
    void getExecutionResultReturnsNullForEmptyResult() throws Exception {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        setField("executionStateStore", executionStateStore);
        ExecutionRecord<Object, Object> record = new ExecutionRecord<>(
            "tenant-1",
            "exec-1",
            "key-1",
            ExecutionStatus.SUCCEEDED,
            1L,
            0,
            0,
            null,
            0L,
            0L,
            null,
            "input",
            java.util.List.of(),
            null,
            null,
            1L,
            1L,
            99999999L);
        when(executionStateStore.getExecution("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));

        Object result = service.getExecutionResult("tenant-1", "exec-1", String.class, false)
            .await().indefinitely();

        assertNull(result);
    }

    @Test
    void executePipelineAsyncUsesDefaultTenantWhenNull() throws Exception {
        configureQueueModeDefaults();
        setField("executionStateStore", executionStateStore);
        setField("workDispatcher", workDispatcher);
        when(orchestratorConfig.defaultTenant()).thenReturn("default-tenant");
        when(executionStateStore.createOrGetExecution(any()))
            .thenReturn(Uni.createFrom().item(new CreateExecutionResult(
                createRecord("default-tenant", "exec-1", "key-1"),
                false)));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

        service.executePipelineAsync("input", null, null).await().indefinitely();

        ArgumentCaptor<org.pipelineframework.orchestrator.ExecutionCreateCommand> captor =
            ArgumentCaptor.forClass(org.pipelineframework.orchestrator.ExecutionCreateCommand.class);
        verify(executionStateStore).createOrGetExecution(captor.capture());
        assertEquals("default-tenant", captor.getValue().tenantId());
    }

    @Test
    void executePipelineAsyncNormalizesBlankTenant() throws Exception {
        configureQueueModeDefaults();
        setField("executionStateStore", executionStateStore);
        setField("workDispatcher", workDispatcher);
        when(orchestratorConfig.defaultTenant()).thenReturn("default-tenant");
        when(executionStateStore.createOrGetExecution(any()))
            .thenReturn(Uni.createFrom().item(new CreateExecutionResult(
                createRecord("default-tenant", "exec-1", "key-1"),
                false)));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

        service.executePipelineAsync("input", "  ", null).await().indefinitely();

        ArgumentCaptor<org.pipelineframework.orchestrator.ExecutionCreateCommand> captor =
            ArgumentCaptor.forClass(org.pipelineframework.orchestrator.ExecutionCreateCommand.class);
        verify(executionStateStore).createOrGetExecution(captor.capture());
        assertEquals("default-tenant", captor.getValue().tenantId());
    }

    @Test
    void executePipelineAsyncSkipsEnqueueForDuplicateExecution() throws Exception {
        configureQueueModeDefaults();
        setField("executionStateStore", executionStateStore);
        setField("workDispatcher", workDispatcher);
        when(executionStateStore.createOrGetExecution(any()))
            .thenReturn(Uni.createFrom().item(new CreateExecutionResult(
                createRecord("tenant-1", "exec-1", "key-1"),
                true)));

        service.executePipelineAsync("input", "tenant-1", null).await().indefinitely();

        verify(executionStateStore).createOrGetExecution(any());
        org.mockito.Mockito.verify(workDispatcher, org.mockito.Mockito.never()).enqueueNow(any());
    }

    @Test
    void executePipelineAsyncValidatesInputMustBeReactiveType() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);

        Uni<?> result = service.executePipelineAsync(123, "tenant-1", null);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> result.await().indefinitely());
        assertTrue(error.getMessage().contains("Pipeline input must be Uni or Multi"));
    }

    @Test
    void executePipelineAsyncRequiresClientKeyWhenPolicyIsClientKeyRequired() throws Exception {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.executionTtlDays()).thenReturn(7);
        when(orchestratorConfig.idempotencyPolicy()).thenReturn(OrchestratorIdempotencyPolicy.CLIENT_KEY_REQUIRED);
        setField("executionStateStore", executionStateStore);
        setField("workDispatcher", workDispatcher);

        Uni<?> result = service.executePipelineAsync("input", "tenant-1", null);

        jakarta.ws.rs.BadRequestException error = assertThrows(jakarta.ws.rs.BadRequestException.class,
            () -> result.await().indefinitely());
        assertTrue(error.getMessage().contains("Idempotency-Key"));
    }
}