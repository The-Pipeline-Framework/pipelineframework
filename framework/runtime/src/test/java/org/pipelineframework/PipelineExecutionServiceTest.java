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
}
