package org.pipelineframework;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.awaitable.AwaitSuspendedException;
import org.pipelineframework.orchestrator.DeadLetterEnvelope;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.TransitionFailureEnvelope;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.ControlPlaneProjection;
import org.pipelineframework.orchestrator.controlplane.InMemoryControlPlaneJournal;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.step.NonRetryableException;
import org.pipelineframework.runtime.core.resilience.CircuitIdentity;
import org.pipelineframework.runtime.core.resilience.CircuitOpen;
import org.pipelineframework.runtime.core.resilience.CircuitOpenException;
import org.pipelineframework.runtime.core.resilience.CircuitScope;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class QueueAsyncFailureMatrixTest {

    private ExecutionFailureHandler failureHandler;

    @Mock
    private PipelineOrchestratorConfig orchestratorConfig;

    @Mock
    private ExecutionStateStore executionStateStore;

    @Mock
    private WorkDispatcher workDispatcher;

    @Mock
    private DeadLetterPublisher deadLetterPublisher;

    @BeforeEach
    void setUp() {
        failureHandler = new ExecutionFailureHandler();
        failureHandler.orchestratorConfig = orchestratorConfig;
    }

    @Test
    void retryPathCommitsAndEnqueuesDelayedWork() {
        configureRetryDefaults();
        ExecutionRecord<Object, Object> record = record("tenant-a", "exec-1", 0L, 0);
        when(executionStateStore.scheduleRetry(
            anyString(), anyString(), anyLong(), anyInt(), anyLong(), anyString(), anyString(), anyString(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));
        when(workDispatcher.enqueueDelayed(any(), any()))
            .thenReturn(Uni.createFrom().voidItem());

        assertDoesNotThrow(() -> failureHandler.handleExecutionFailure(
            record,
            "exec-1:0:0",
            new RuntimeException("boom"),
            executionStateStore,
            workDispatcher,
            deadLetterPublisher).await().atMost(Duration.ofSeconds(3)));

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
    void circuitOpenDefersWithoutConsumingTheRemoteAttemptBudget() {
        when(orchestratorConfig.retryDelay()).thenReturn(Duration.ofSeconds(5));
        when(orchestratorConfig.retryMultiplier()).thenReturn(2.0d);
        when(orchestratorConfig.maxCircuitDeferral()).thenReturn(Optional.of(Duration.ofMinutes(5)));
        ExecutionRecord<Object, Object> record = record("tenant-a", "exec-circuit", 3L, 0);
        when(executionStateStore.deferCircuit(
            anyString(), anyString(), anyLong(), anyLong(), anyString(), anyString(), anyString(), anyString(),
            anyLong(), anyInt(), anyLong())).thenReturn(Uni.createFrom().item(Optional.of(record)));
        when(workDispatcher.enqueueDelayed(any(), any())).thenReturn(Uni.createFrom().voidItem());
        Instant notBefore = Instant.now().plusSeconds(30);

        assertDoesNotThrow(() -> failureHandler.handleExecutionFailure(
            record,
            "exec-circuit:0:0",
            new CircuitOpenException(new CircuitOpen(new CircuitIdentity("pricing"), CircuitScope.SHARED_DEPENDENCY, notBefore)),
            executionStateStore,
            workDispatcher,
            deadLetterPublisher).await().atMost(Duration.ofSeconds(3)));

        verify(executionStateStore).deferCircuit(
            eq("tenant-a"), eq("exec-circuit"), eq(3L), anyLong(), eq("exec-circuit:0:0"),
            eq("pricing"), eq("circuit_open"), anyString(), anyLong(), eq(1), anyLong());
        verify(executionStateStore, never()).scheduleRetry(
            anyString(), anyString(), anyLong(), anyInt(), anyLong(), anyString(), anyString(), anyString(), anyLong());
    }

    @Test
    void circuitOpenDeferralNeverSchedulesPastItsConfiguredDeadline() {
        when(orchestratorConfig.retryDelay()).thenReturn(Duration.ofSeconds(5));
        when(orchestratorConfig.retryMultiplier()).thenReturn(2.0d);
        Duration maximum = Duration.ofSeconds(5);
        when(orchestratorConfig.maxCircuitDeferral()).thenReturn(Optional.of(maximum));
        ExecutionRecord<Object, Object> record = record("tenant-a", "exec-circuit-deadline", 3L, 0);
        when(executionStateStore.deferCircuit(
            anyString(), anyString(), anyLong(), anyLong(), anyString(), anyString(), anyString(), anyString(),
            anyLong(), anyInt(), anyLong())).thenReturn(Uni.createFrom().item(Optional.of(record)));
        when(workDispatcher.enqueueDelayed(any(), any())).thenReturn(Uni.createFrom().voidItem());
        Instant notBefore = Instant.now().plusSeconds(30);

        failureHandler.handleExecutionFailure(
            record,
            "exec-circuit-deadline:0:0",
            new CircuitOpenException(new CircuitOpen(new CircuitIdentity("pricing"), CircuitScope.SHARED_DEPENDENCY, notBefore)),
            executionStateStore,
            workDispatcher,
            deadLetterPublisher).await().atMost(Duration.ofSeconds(3));

        ArgumentCaptor<Long> nextDue = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> deferredAt = ArgumentCaptor.forClass(Long.class);
        verify(executionStateStore).deferCircuit(
            eq("tenant-a"), eq("exec-circuit-deadline"), eq(3L), nextDue.capture(), eq("exec-circuit-deadline:0:0"),
            eq("pricing"), eq("circuit_open"), anyString(), anyLong(), eq(1), deferredAt.capture());
        assertEquals(deferredAt.getValue() + maximum.toMillis(), nextDue.getValue());
    }

    @Test
    void terminalPathClassifiesNonRetryableFailure() {
        when(orchestratorConfig.maxRetries()).thenReturn(0);
        InMemoryControlPlaneJournal journal = new InMemoryControlPlaneJournal();
        failureHandler.segmentBoundaryLedger = new SegmentBoundaryLedger(journal);
        ExecutionRecord<Object, Object> record = record("tenant-a", "exec-9", 3L, 0);
        when(executionStateStore.markTerminalFailure(
            anyString(), anyString(), anyLong(), any(), anyString(), anyString(), anyString(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));
        when(deadLetterPublisher.publish(any())).thenReturn(Uni.createFrom().voidItem());

        assertDoesNotThrow(() -> failureHandler.handleExecutionFailure(
            record,
            "exec-9:0:0",
            new NonRetryableException("bad payload"),
            executionStateStore,
            workDispatcher,
            deadLetterPublisher).await().atMost(Duration.ofSeconds(3)));

        ArgumentCaptor<DeadLetterEnvelope> envelopeCaptor = ArgumentCaptor.forClass(DeadLetterEnvelope.class);
        verify(deadLetterPublisher).publish(envelopeCaptor.capture());
        DeadLetterEnvelope envelope = envelopeCaptor.getValue();
        assertEquals("non_retryable", envelope.terminalReason());
        assertFalse(envelope.retryable());
        assertEquals("NonRetryableException", envelope.errorCode());
        ControlPlaneProjection projection = journal.projection("tenant-a", "exec-9").await().indefinitely();
        assertTrue(projection.factKeys().contains("run-failed:exec-9:NonRetryableException"));
    }

    @Test
    void wrappedNonRetryableFailureUsesClassifiedThrowableMetadata() {
        when(orchestratorConfig.maxRetries()).thenReturn(3);
        ExecutionRecord<Object, Object> record = record("tenant-a", "exec-11", 4L, 0);
        when(executionStateStore.markTerminalFailure(
            anyString(), anyString(), anyLong(), any(), anyString(), anyString(), anyString(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));
        when(deadLetterPublisher.publish(any())).thenReturn(Uni.createFrom().voidItem());

        RuntimeException wrapped = new RuntimeException("wrapper", new NonRetryableException("inner-non-retryable"));
        assertDoesNotThrow(() -> failureHandler.handleExecutionFailure(
            record,
            "exec-11:0:0",
            wrapped,
            executionStateStore,
            workDispatcher,
            deadLetterPublisher).await().atMost(Duration.ofSeconds(3)));

        verify(executionStateStore, never()).scheduleRetry(
            anyString(), anyString(), anyLong(), anyInt(), anyLong(), anyString(), anyString(), anyString(), anyLong());
        verify(executionStateStore).markTerminalFailure(
            eq("tenant-a"),
            eq("exec-11"),
            eq(4L),
            eq(ExecutionStatus.FAILED),
            eq("exec-11:0:0"),
            eq("NonRetryableException"),
            eq("inner-non-retryable"),
            anyLong());

        ArgumentCaptor<DeadLetterEnvelope> envelopeCaptor = ArgumentCaptor.forClass(DeadLetterEnvelope.class);
        verify(deadLetterPublisher).publish(envelopeCaptor.capture());
        DeadLetterEnvelope envelope = envelopeCaptor.getValue();
        assertEquals("NonRetryableException", envelope.errorCode());
        assertEquals("inner-non-retryable", envelope.errorMessage());
        assertFalse(envelope.retryable());
        assertEquals("non_retryable", envelope.terminalReason());
    }

    @Test
    void controlFlowFailureIsNotRetriedWhenWrappedInCompositeException() {
        when(orchestratorConfig.maxRetries()).thenReturn(3);
        ExecutionRecord<Object, Object> record = record("tenant-a", "exec-await", 7L, 0);
        when(executionStateStore.markTerminalFailure(
            anyString(), anyString(), anyLong(), any(), anyString(), anyString(), anyString(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));
        when(deadLetterPublisher.publish(any())).thenReturn(Uni.createFrom().voidItem());

        CompositeException failure = new CompositeException(
            new IllegalStateException("stream cancelled"),
            new AwaitSuspendedException("tenant-a", "exec-await", "unit-1", 2));

        assertDoesNotThrow(() -> failureHandler.handleExecutionFailure(
            record,
            "exec-await:0:0",
            failure,
            executionStateStore,
            workDispatcher,
            deadLetterPublisher).await().atMost(Duration.ofSeconds(3)));

        verify(executionStateStore, never()).scheduleRetry(
            anyString(), anyString(), anyLong(), anyInt(), anyLong(), anyString(), anyString(), anyString(), anyLong());
        verify(executionStateStore).markTerminalFailure(
            eq("tenant-a"),
            eq("exec-await"),
            eq(7L),
            eq(ExecutionStatus.FAILED),
            eq("exec-await:0:0"),
            eq("AwaitSuspendedException"),
            anyString(),
            anyLong());
    }

    @Test
    void transitionFailureEnvelopePreservesNonRetryableClassification() {
        when(orchestratorConfig.maxRetries()).thenReturn(3);
        ExecutionRecord<Object, Object> record = record("tenant-a", "exec-13", 6L, 0);
        when(executionStateStore.markTerminalFailure(
            anyString(), anyString(), anyLong(), any(), anyString(), anyString(), anyString(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));
        when(deadLetterPublisher.publish(any())).thenReturn(Uni.createFrom().voidItem());

        assertDoesNotThrow(() -> failureHandler.handleExecutionFailure(
            record,
            "exec-13:0:0",
            new TransitionFailureEnvelope(NonRetryableException.class.getName(), "do not retry").toException(),
            executionStateStore,
            workDispatcher,
            deadLetterPublisher).await().atMost(Duration.ofSeconds(3)));

        verify(executionStateStore, never()).scheduleRetry(
            anyString(), anyString(), anyLong(), anyInt(), anyLong(), anyString(), anyString(), anyString(), anyLong());
        verify(executionStateStore).markTerminalFailure(
            eq("tenant-a"),
            eq("exec-13"),
            eq(6L),
            eq(ExecutionStatus.FAILED),
            eq("exec-13:0:0"),
            eq("NonRetryableException"),
            eq("do not retry"),
            anyLong());
    }

    @Test
    void transitionFailureEnvelopePreservesNonRetryableSubclassClassification() {
        RuntimeException exception = new TransitionFailureEnvelope(
            CustomNonRetryableException.class.getName(),
            "custom non-retryable").toException();

        assertTrue(exception instanceof NonRetryableException);
        assertEquals("custom non-retryable", exception.getMessage());
    }

    @Test
    void retryableFailureWithNoBudgetPublishesRetryExhaustedTerminal() {
        when(orchestratorConfig.maxRetries()).thenReturn(0);
        ExecutionRecord<Object, Object> record = record("tenant-a", "exec-12", 5L, 0);
        when(executionStateStore.markTerminalFailure(
            anyString(), anyString(), anyLong(), any(), anyString(), anyString(), anyString(), anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(record)));
        when(deadLetterPublisher.publish(any())).thenReturn(Uni.createFrom().voidItem());

        assertDoesNotThrow(() -> failureHandler.handleExecutionFailure(
            record,
            "exec-12:0:0",
            new IllegalStateException("terminal"),
            executionStateStore,
            workDispatcher,
            deadLetterPublisher).await().atMost(Duration.ofSeconds(3)));

        ArgumentCaptor<DeadLetterEnvelope> envelopeCaptor = ArgumentCaptor.forClass(DeadLetterEnvelope.class);
        verify(deadLetterPublisher).publish(envelopeCaptor.capture());
        DeadLetterEnvelope envelope = envelopeCaptor.getValue();
        assertTrue(envelope.retryable());
        assertEquals("retry_exhausted", envelope.terminalReason());
    }

    private void configureRetryDefaults() {
        when(orchestratorConfig.maxRetries()).thenReturn(3);
        when(orchestratorConfig.retryDelay()).thenReturn(Duration.ofSeconds(5));
        when(orchestratorConfig.retryMultiplier()).thenReturn(2.0d);
    }

    private static ExecutionRecord<Object, Object> record(String tenantId, String executionId, long version, int attempt) {
        return new ExecutionRecord<>(
            tenantId,
            executionId,
            executionId + "-key",
            ExecutionResultShape.SINGLE,
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
            null,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            System.currentTimeMillis() / 1000 + 3600);
    }

    private static final class CustomNonRetryableException extends NonRetryableException {
        private CustomNonRetryableException(String message) {
            super(message);
        }
    }
}
