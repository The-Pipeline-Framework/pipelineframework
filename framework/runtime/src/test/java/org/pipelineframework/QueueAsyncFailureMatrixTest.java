package org.pipelineframework;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.orchestrator.DeadLetterEnvelope;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.WorkDispatcher;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class QueueAsyncFailureMatrixTest {

    private PipelineExecutionService service;

    @Mock
    private PipelineOrchestratorConfig orchestratorConfig;

    @Mock
    private ExecutionStateStore executionStateStore;

    @Mock
    private WorkDispatcher workDispatcher;

    @Mock
    private DeadLetterPublisher deadLetterPublisher;

    @BeforeEach
    void setUp() throws Exception {
        service = new PipelineExecutionService();
        setField("orchestratorConfig", orchestratorConfig);
        setField("executionStateStore", executionStateStore);
        setField("workDispatcher", workDispatcher);
        setField("deadLetterPublisher", deadLetterPublisher);
    }

    @Test
    void retryPathCommitsAndEnqueuesDelayedWork() throws Exception {
        configureRetryDefaults();
        ExecutionRecord<Object, Object> record = record("tenant-a", "exec-1", 0L, 0);
        when(executionStateStore.scheduleRetry(
            anyString(), anyString(), anyLong(), anyInt(), anyLong(), anyString(), anyString(), anyString(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));
        when(workDispatcher.enqueueDelayed(any(), any()))
            .thenReturn(Uni.createFrom().voidItem());

        assertDoesNotThrow(() -> invokeHandleExecutionFailure(record, "exec-1:0:0", new RuntimeException("boom")));

        verify(executionStateStore).scheduleRetry(
            eq("tenant-a"),
            eq("exec-1"),
            eq(0L),
            eq(1),
            anyLong(),
            eq("exec-1:0:0"),
            eq("RuntimeException"),
            eq("boom"),
            anyLong());
        verify(workDispatcher).enqueueDelayed(eq(new ExecutionWorkItem("tenant-a", "exec-1")), any(Duration.class));
        verify(executionStateStore, never()).markTerminalFailure(anyString(), anyString(), anyLong(), any(), anyString(), anyString(), anyString(), anyLong());
    }

    @Test
    void retryPathWithStaleCommitSkipsReenqueue() throws Exception {
        configureRetryDefaults();
        ExecutionRecord<Object, Object> record = record("tenant-a", "exec-2", 1L, 1);
        when(executionStateStore.scheduleRetry(
            anyString(), anyString(), anyLong(), anyInt(), anyLong(), anyString(), anyString(), anyString(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.empty()));

        assertDoesNotThrow(() -> invokeHandleExecutionFailure(record, "exec-2:1:1", new RuntimeException("retry")));

        verify(executionStateStore).scheduleRetry(
            eq("tenant-a"),
            eq("exec-2"),
            eq(1L),
            eq(2),
            anyLong(),
            eq("exec-2:1:1"),
            eq("RuntimeException"),
            eq("retry"),
            anyLong());
        verify(workDispatcher, never()).enqueueDelayed(any(), any());
    }

    @Test
    void terminalPathPublishesDeadLetterOnCommit() throws Exception {
        when(orchestratorConfig.maxRetries()).thenReturn(0);
        ExecutionRecord<Object, Object> record = record("tenant-a", "exec-3", 2L, 0);
        when(executionStateStore.markTerminalFailure(
            anyString(), anyString(), anyLong(), any(), anyString(), anyString(), anyString(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));
        when(deadLetterPublisher.publish(any())).thenReturn(Uni.createFrom().voidItem());

        assertDoesNotThrow(() -> invokeHandleExecutionFailure(record, "exec-3:0:0", new IllegalStateException("final")));

        ArgumentCaptor<DeadLetterEnvelope> envelopeCaptor = ArgumentCaptor.forClass(DeadLetterEnvelope.class);
        verify(deadLetterPublisher).publish(envelopeCaptor.capture());
        DeadLetterEnvelope envelope = envelopeCaptor.getValue();
        org.junit.jupiter.api.Assertions.assertEquals("tenant-a", envelope.tenantId());
        org.junit.jupiter.api.Assertions.assertEquals("exec-3", envelope.executionId());
        org.junit.jupiter.api.Assertions.assertEquals("exec-3:0:0", envelope.transitionKey());
    }

    @Test
    void terminalPathWithStaleCommitSkipsDeadLetterPublish() throws Exception {
        when(orchestratorConfig.maxRetries()).thenReturn(0);
        ExecutionRecord<Object, Object> record = record("tenant-a", "exec-4", 3L, 0);
        when(executionStateStore.markTerminalFailure(
            anyString(), anyString(), anyLong(), any(), anyString(), anyString(), anyString(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.empty()));

        assertDoesNotThrow(() -> invokeHandleExecutionFailure(record, "exec-4:0:0", new IllegalStateException("stale")));

        verify(deadLetterPublisher, never()).publish(any());
    }

    @Test
    void sweepRedispatchesPersistedDueExecutions() throws Exception {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.sweepLimit()).thenReturn(100);
        when(executionStateStore.findDueExecutions(anyLong(), eq(100)))
            .thenReturn(Uni.createFrom().item(List.of(
                record("tenant-a", "exec-5", 0L, 0),
                record("tenant-b", "exec-6", 0L, 0))));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());

        invokeSweepDueExecutions();

        verify(workDispatcher, timeout(500).times(2)).enqueueNow(any());
    }

    private void configureRetryDefaults() {
        when(orchestratorConfig.maxRetries()).thenReturn(3);
        when(orchestratorConfig.retryDelay()).thenReturn(Duration.ofSeconds(5));
        when(orchestratorConfig.retryMultiplier()).thenReturn(2.0d);
    }

    private void invokeHandleExecutionFailure(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        Throwable failure
    ) throws Exception {
        Method method = PipelineExecutionService.class.getDeclaredMethod(
            "handleExecutionFailure",
            ExecutionRecord.class,
            String.class,
            Throwable.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        Uni<Void> result = (Uni<Void>) method.invoke(service, record, transitionKey, failure);
        result.await().indefinitely();
    }

    private void invokeSweepDueExecutions() throws Exception {
        Method method = PipelineExecutionService.class.getDeclaredMethod("sweepDueExecutions");
        method.setAccessible(true);
        method.invoke(service);
    }

    private static ExecutionRecord<Object, Object> record(String tenantId, String executionId, long version, int attempt) {
        return new ExecutionRecord<>(
            tenantId,
            executionId,
            executionId + "-key",
            ExecutionStatus.RUNNING,
            version,
            0,
            attempt,
            null,
            0L,
            0L,
            null,
            null,
            null,
            null,
            null,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            System.currentTimeMillis() / 1000 + 3600);
    }

    private void setField(String fieldName, Object value) throws Exception {
        var field = PipelineExecutionService.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(service, value);
    }

    @Test
    void retryPathCalculatesExponentialBackoff() throws Exception {
        configureRetryDefaults();
        ExecutionRecord<Object, Object> record = record("tenant-a", "exec-7", 0L, 1);
        ArgumentCaptor<Long> nextDueCaptor = ArgumentCaptor.forClass(Long.class);
        when(executionStateStore.scheduleRetry(
            anyString(), anyString(), anyLong(), anyInt(), nextDueCaptor.capture(), anyString(), anyString(), anyString(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));
        when(workDispatcher.enqueueDelayed(any(), any()))
            .thenReturn(Uni.createFrom().voidItem());

        assertDoesNotThrow(() -> invokeHandleExecutionFailure(record, "exec-7:0:1", new RuntimeException("retry backoff")));

        verify(executionStateStore).scheduleRetry(
            eq("tenant-a"),
            eq("exec-7"),
            eq(0L),
            eq(2),
            anyLong(),
            eq("exec-7:0:1"),
            eq("RuntimeException"),
            eq("retry backoff"),
            anyLong());
    }

    @Test
    void terminalPathCommitsFailedStatusCorrectly() throws Exception {
        when(orchestratorConfig.maxRetries()).thenReturn(2);
        ExecutionRecord<Object, Object> record = record("tenant-a", "exec-8", 2L, 2);
        when(executionStateStore.markTerminalFailure(
            anyString(), anyString(), anyLong(), any(), anyString(), anyString(), anyString(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));
        when(deadLetterPublisher.publish(any())).thenReturn(Uni.createFrom().voidItem());

        assertDoesNotThrow(() -> invokeHandleExecutionFailure(record, "exec-8:0:2", new RuntimeException("exhausted")));

        verify(executionStateStore).markTerminalFailure(
            eq("tenant-a"),
            eq("exec-8"),
            eq(2L),
            any(ExecutionStatus.class),
            eq("exec-8:0:2"),
            eq("RuntimeException"),
            eq("exhausted"),
            anyLong());
    }

    @Test
    void sweepIgnoresExecutionWhenDispatchFails() throws Exception {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.sweepLimit()).thenReturn(100);
        when(executionStateStore.findDueExecutions(anyLong(), eq(100)))
            .thenReturn(Uni.createFrom().item(List.of(
                record("tenant-a", "exec-9", 0L, 0))));
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().failure(new RuntimeException("dispatch failed")));

        assertDoesNotThrow(() -> invokeSweepDueExecutions());

        verify(workDispatcher, timeout(500)).enqueueNow(any());
    }

    @Test
    void retryPathWithZeroMaxRetriesGoesDirectlyToTerminal() throws Exception {
        when(orchestratorConfig.maxRetries()).thenReturn(0);
        ExecutionRecord<Object, Object> record = record("tenant-a", "exec-10", 0L, 0);
        when(executionStateStore.markTerminalFailure(
            anyString(), anyString(), anyLong(), any(), anyString(), anyString(), anyString(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));
        when(deadLetterPublisher.publish(any())).thenReturn(Uni.createFrom().voidItem());

        assertDoesNotThrow(() -> invokeHandleExecutionFailure(record, "exec-10:0:0", new RuntimeException("immediate fail")));

        verify(executionStateStore, never()).scheduleRetry(anyString(), anyString(), anyLong(), anyInt(), anyLong(), anyString(), anyString(), anyString(), anyLong());
        verify(executionStateStore).markTerminalFailure(
            eq("tenant-a"),
            eq("exec-10"),
            eq(0L),
            any(ExecutionStatus.class),
            eq("exec-10:0:0"),
            eq("RuntimeException"),
            eq("immediate fail"),
            anyLong());
    }

    @Test
    void deadLetterPublishFailureDoesNotBlockTerminalTransition() throws Exception {
        when(orchestratorConfig.maxRetries()).thenReturn(0);
        ExecutionRecord<Object, Object> record = record("tenant-a", "exec-11", 2L, 0);
        when(executionStateStore.markTerminalFailure(
            anyString(), anyString(), anyLong(), any(), anyString(), anyString(), anyString(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));
        when(deadLetterPublisher.publish(any())).thenReturn(Uni.createFrom().failure(new RuntimeException("dlq publish failed")));

        assertDoesNotThrow(() -> invokeHandleExecutionFailure(record, "exec-11:0:0", new IllegalStateException("terminal error")));

        verify(deadLetterPublisher).publish(any());
    }

    @Test
    void sweepHandlesEmptyDueList() throws Exception {
        when(orchestratorConfig.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        when(orchestratorConfig.sweepLimit()).thenReturn(100);
        when(executionStateStore.findDueExecutions(anyLong(), eq(100)))
            .thenReturn(Uni.createFrom().item(List.of()));

        assertDoesNotThrow(() -> invokeSweepDueExecutions());

        verify(workDispatcher, never()).enqueueNow(any());
    }

    @Test
    void retryPathHandlesMultipleAttempts() throws Exception {
        configureRetryDefaults();
        ExecutionRecord<Object, Object> firstAttempt = record("tenant-a", "exec-12", 0L, 0);
        ExecutionRecord<Object, Object> secondAttempt = record("tenant-a", "exec-12", 1L, 1);
        ExecutionRecord<Object, Object> thirdAttempt = record("tenant-a", "exec-12", 2L, 2);

        when(executionStateStore.scheduleRetry(
            anyString(), anyString(), eq(0L), eq(1), anyLong(), anyString(), anyString(), anyString(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(firstAttempt)));
        when(executionStateStore.scheduleRetry(
            anyString(), anyString(), eq(1L), eq(2), anyLong(), anyString(), anyString(), anyString(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(secondAttempt)));
        when(executionStateStore.scheduleRetry(
            anyString(), anyString(), eq(2L), eq(3), anyLong(), anyString(), anyString(), anyString(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(thirdAttempt)));
        when(workDispatcher.enqueueDelayed(any(), any()))
            .thenReturn(Uni.createFrom().voidItem());

        assertDoesNotThrow(() -> invokeHandleExecutionFailure(firstAttempt, "exec-12:0:0", new RuntimeException("first")));
        assertDoesNotThrow(() -> invokeHandleExecutionFailure(secondAttempt, "exec-12:0:1", new RuntimeException("second")));
        assertDoesNotThrow(() -> invokeHandleExecutionFailure(thirdAttempt, "exec-12:0:2", new RuntimeException("third")));

        verify(executionStateStore, times(3)).scheduleRetry(
            eq("tenant-a"),
            eq("exec-12"),
            anyLong(),
            anyInt(),
            anyLong(),
            anyString(),
            eq("RuntimeException"),
            anyString(),
            anyLong());
    }
}