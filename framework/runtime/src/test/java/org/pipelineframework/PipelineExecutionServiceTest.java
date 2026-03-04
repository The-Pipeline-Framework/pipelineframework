package org.pipelineframework;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.orchestrator.*;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;
import org.pipelineframework.telemetry.PipelineTelemetry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PipelineExecutionServiceTest {

    private PipelineExecutionService service;

    @Mock
    private PipelineConfig pipelineConfig;

    @Mock
    private PipelineRunner pipelineRunner;

    @Mock
    private HealthCheckService healthCheckService;

    @Mock
    private PipelineTelemetry telemetry;

    @Mock
    private PipelineOrchestratorConfig orchestratorConfig;

    @Mock
    private Instance<ExecutionStateStore> executionStateStores;

    @Mock
    private Instance<WorkDispatcher> workDispatchers;

    @Mock
    private Instance<DeadLetterPublisher> deadLetterPublishers;

    @Mock
    private ExecutionStateStore executionStateStore;

    @Mock
    private WorkDispatcher workDispatcher;

    @Mock
    private DeadLetterPublisher deadLetterPublisher;

    @BeforeEach
    void setUp() {
        service = new PipelineExecutionService();
        injectDependencies();

        // Default config for non-queue mode
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.DISABLED);
    }

    @Test
    void executePipelineStreamingReturnsMulti() {
        Uni<String> input = Uni.createFrom().item("test");
        Multi<String> output = Multi.createFrom().item("result");

        when(healthCheckService.checkHealthOfDependentServices(any())).thenReturn(true);
        when(pipelineRunner.run(any(), any())).thenReturn(output);

        Multi<String> result = service.executePipelineStreaming(input);

        assertNotNull(result);
        assertEquals("result", result.collect().first().await().indefinitely());
    }

    @Test
    void executePipelineUnaryReturnsUni() {
        Uni<String> input = Uni.createFrom().item("test");
        Uni<String> output = Uni.createFrom().item("result");

        when(healthCheckService.checkHealthOfDependentServices(any())).thenReturn(true);
        when(pipelineRunner.run(any(), any())).thenReturn(output);

        Uni<String> result = service.executePipelineUnary(input);

        assertNotNull(result);
        assertEquals("result", result.await().indefinitely());
    }

    @Test
    void executePipelineUnaryFailsForStreamingOutput() {
        Uni<String> input = Uni.createFrom().item("test");
        Multi<String> output = Multi.createFrom().items("result1", "result2");

        when(healthCheckService.checkHealthOfDependentServices(any())).thenReturn(true);
        when(pipelineRunner.run(any(), any())).thenReturn(output);

        Uni<String> result = service.executePipelineUnary(input);

        assertThrows(IllegalStateException.class, () -> result.await().indefinitely());
    }

    @Test
    void executePipelineAsyncRequiresQueueMode() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.DISABLED);

        Uni<RunAsyncAcceptedDto> result = service.executePipelineAsync("input", "tenant1", "key1");

        assertThrows(IllegalStateException.class, () -> result.await().indefinitely());
    }

    @Test
    void executePipelineAsyncCreatesExecution() {
        setupQueueMode();

        Object input = "test-input";
        ExecutionRecord record = createTestRecord("exec1", ExecutionStatus.PENDING);
        CreateExecutionResult createResult = new CreateExecutionResult(record, false);

        when(executionStateStore.createOrGetExecution(any())).thenReturn(Uni.createFrom().item(createResult));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

        Uni<RunAsyncAcceptedDto> result = service.executePipelineAsync(input, "tenant1", "key1");

        RunAsyncAcceptedDto dto = result.await().indefinitely();
        assertNotNull(dto);
        assertEquals("exec1", dto.executionId());
        assertFalse(dto.duplicate());
        assertTrue(dto.statusUrl().contains("exec1"));
    }

    @Test
    void executePipelineAsyncHandlesDuplicateExecution() {
        setupQueueMode();

        Object input = "test-input";
        ExecutionRecord record = createTestRecord("exec2", ExecutionStatus.RUNNING);
        CreateExecutionResult createResult = new CreateExecutionResult(record, true);

        when(executionStateStore.createOrGetExecution(any())).thenReturn(Uni.createFrom().item(createResult));

        Uni<RunAsyncAcceptedDto> result = service.executePipelineAsync(input, "tenant1", "key2");

        RunAsyncAcceptedDto dto = result.await().indefinitely();
        assertNotNull(dto);
        assertTrue(dto.duplicate());
        verify(workDispatcher, never()).enqueueNow(any());
    }

    @Test
    void getExecutionStatusRequiresQueueMode() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.DISABLED);

        Uni<ExecutionStatusDto> result = service.getExecutionStatus("tenant1", "exec1");

        assertThrows(IllegalStateException.class, () -> result.await().indefinitely());
    }

    @Test
    void getExecutionStatusReturnsStatus() {
        setupQueueMode();

        ExecutionRecord record = createTestRecord("exec1", ExecutionStatus.RUNNING);
        when(executionStateStore.getExecution(any(), any()))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));

        Uni<ExecutionStatusDto> result = service.getExecutionStatus("tenant1", "exec1");

        ExecutionStatusDto dto = result.await().indefinitely();
        assertNotNull(dto);
        assertEquals("exec1", dto.executionId());
        assertEquals(ExecutionStatus.RUNNING, dto.status());
    }

    @Test
    void getExecutionStatusThrowsNotFoundForMissingExecution() {
        setupQueueMode();

        when(executionStateStore.getExecution(any(), any()))
            .thenReturn(Uni.createFrom().item(Optional.empty()));

        Uni<ExecutionStatusDto> result = service.getExecutionStatus("tenant1", "exec1");

        assertThrows(jakarta.ws.rs.NotFoundException.class, () -> result.await().indefinitely());
    }

    @Test
    void getExecutionResultReturnsUnaryResult() {
        setupQueueMode();

        ExecutionRecord record = createTestRecord("exec1", ExecutionStatus.SUCCEEDED);
        record = new ExecutionRecord(
            record.tenantId(),
            record.executionId(),
            record.executionKey(),
            record.status(),
            record.currentStepIndex(),
            record.attempt(),
            record.version(),
            record.leaseOwner(),
            record.errorCode(),
            record.errorMessage(),
            List.of("result"),
            record.nextDueEpochMs(),
            record.updatedAtEpochMs(),
            record.ttlEpochS());

        when(executionStateStore.getExecution(any(), any()))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));

        Uni<String> result = service.getExecutionResult("tenant1", "exec1", String.class, false);

        assertEquals("result", result.await().indefinitely());
    }

    @Test
    void getExecutionResultReturnsStreamingResult() {
        setupQueueMode();

        List<String> resultList = List.of("result1", "result2");
        ExecutionRecord record = createTestRecord("exec1", ExecutionStatus.SUCCEEDED);
        record = new ExecutionRecord(
            record.tenantId(),
            record.executionId(),
            record.executionKey(),
            record.status(),
            record.currentStepIndex(),
            record.attempt(),
            record.version(),
            record.leaseOwner(),
            record.errorCode(),
            record.errorMessage(),
            resultList,
            record.nextDueEpochMs(),
            record.updatedAtEpochMs(),
            record.ttlEpochS());

        when(executionStateStore.getExecution(any(), any()))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));

        Uni<List<String>> result = service.getExecutionResult("tenant1", "exec1", String.class, true);

        assertEquals(resultList, result.await().indefinitely());
    }

    @Test
    void processExecutionWorkItemHandlesSuccess() {
        setupQueueMode();

        ExecutionRecord record = createTestRecord("exec1", ExecutionStatus.RUNNING);
        ExecutionWorkItem workItem = new ExecutionWorkItem("tenant1", "exec1");

        when(executionStateStore.claimLease(any(), any(), any(), anyLong(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));
        when(healthCheckService.checkHealthOfDependentServices(any())).thenReturn(true);
        when(pipelineRunner.run(any(), any())).thenReturn(Multi.createFrom().item("result"));
        when(executionStateStore.markSucceeded(any(), any(), anyLong(), any(), any(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));

        Uni<Void> result = service.processExecutionWorkItem(workItem);

        assertDoesNotThrow(() -> result.await().indefinitely());
        verify(executionStateStore).markSucceeded(any(), any(), anyLong(), any(), any(), anyLong());
    }

    @Test
    void startupHealthStateInitiallyPending() {
        assertEquals(PipelineExecutionService.StartupHealthState.PENDING, service.getStartupHealthState());
    }

    @Test
    void validatesInputShapeRejectsNonReactiveTypes() {
        when(healthCheckService.checkHealthOfDependentServices(any())).thenReturn(true);

        Multi<?> result = service.executePipelineStreaming("invalid-input");

        assertThrows(IllegalArgumentException.class, () -> result.collect().first().await().indefinitely());
    }

    @Test
    void executePipelineStreamingHandlesNullRunnerResult() {
        Uni<String> input = Uni.createFrom().item("test");

        when(healthCheckService.checkHealthOfDependentServices(any())).thenReturn(true);
        when(pipelineRunner.run(any(), any())).thenReturn(null);

        Multi<String> result = service.executePipelineStreaming(input);

        assertThrows(IllegalStateException.class, () -> result.collect().first().await().indefinitely());
    }

    private void setupQueueMode() {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.stateProvider()).thenReturn("test");
        when(orchestratorConfig.dispatcherProvider()).thenReturn("test");
        when(orchestratorConfig.dlqProvider()).thenReturn("test");
        when(orchestratorConfig.leaseMs()).thenReturn(30000L);
        when(orchestratorConfig.maxRetries()).thenReturn(3);
        when(orchestratorConfig.retryDelay()).thenReturn(Duration.ofSeconds(1));
        when(orchestratorConfig.retryMultiplier()).thenReturn(2.0);
        when(orchestratorConfig.defaultTenant()).thenReturn("default");
        when(orchestratorConfig.executionTtlDays()).thenReturn(7);
        when(orchestratorConfig.idempotencyPolicy()).thenReturn(OrchestratorIdempotencyPolicy.OPTIONAL_CLIENT_KEY);

        when(executionStateStores.stream()).thenReturn(java.util.stream.Stream.of(executionStateStore));
        when(executionStateStore.providerName()).thenReturn("test");
        when(executionStateStore.priority()).thenReturn(100);

        when(workDispatchers.stream()).thenReturn(java.util.stream.Stream.of(workDispatcher));
        when(workDispatcher.providerName()).thenReturn("test");
        when(workDispatcher.priority()).thenReturn(100);

        when(deadLetterPublishers.stream()).thenReturn(java.util.stream.Stream.of(deadLetterPublisher));
        when(deadLetterPublisher.providerName()).thenReturn("test");
        when(deadLetterPublisher.priority()).thenReturn(100);

        // Re-inject to trigger queue mode initialization
        service = new PipelineExecutionService();
        injectDependencies();
        try {
            service.initializeQueueMode();
        } catch (Exception ignored) {
            // Ignore errors from initialization
        }
    }

    private ExecutionRecord createTestRecord(String executionId, ExecutionStatus status) {
        return new ExecutionRecord(
            "tenant1",
            executionId,
            "key1",
            status,
            0,
            1,
            1,
            null,
            null,
            null,
            null,
            0L,
            System.currentTimeMillis(),
            System.currentTimeMillis() / 1000 + 86400);
    }

    private void injectDependencies() {
        try {
            setField("pipelineConfig", pipelineConfig);
            setField("pipelineRunner", pipelineRunner);
            setField("healthCheckService", healthCheckService);
            setField("telemetry", telemetry);
            setField("orchestratorConfig", orchestratorConfig);
            setField("executionStateStores", executionStateStores);
            setField("workDispatchers", workDispatchers);
            setField("deadLetterPublishers", deadLetterPublishers);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject dependencies", e);
        }
    }

    private void setField(String fieldName, Object value) throws Exception {
        var field = PipelineExecutionService.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(service, value);
    }
}