package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.orchestrator.DeadLetterPublisher;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.WorkDispatcher;

@ExtendWith(MockitoExtension.class)
class TransitionCommitFlowTest {

    @Mock
    private AwaitCoordinator awaitCoordinator;

    @Mock
    private ExecutionStateStore executionStateStore;

    @Mock
    private WorkDispatcher workDispatcher;

    @Mock
    private DeadLetterPublisher deadLetterPublisher;

    @Mock
    private ExecutionFailureHandler executionFailureHandler;

    @Mock
    private TerminalPublicationBarrier terminalPublicationBarrier;

    @Mock
    private TransitionCommitFlow.Effects effects;

    @Mock
    private AwaitItemContinuationHandler itemContinuationHandler;

    private TransitionCommitFlow runtime;

    @BeforeEach
    void setUp() {
        runtime = new TransitionCommitFlow(
            awaitCoordinator,
            executionStateStore,
            workDispatcher,
            deadLetterPublisher,
            executionFailureHandler,
            terminalPublicationBarrier,
            effects);
    }

    @Test
    void completedTransitionPublishesThroughBarrierBeforeMarkSuccess() {
        ExecutionRecord<Object, Object> record = execution(ExecutionStatus.RUNNING, 1L);
        TransitionResultEnvelope result = TransitionResultEnvelope.completedInProcess(List.of("out-1"));
        when(terminalPublicationBarrier.publishBeforeSuccess(record, result)).thenReturn(Uni.createFrom().voidItem());
        when(executionStateStore.markSucceeded(
            eq(record.tenantId()),
            eq(record.executionId()),
            eq(record.version()),
            eq("transition-1"),
            eq(List.of("out-1")),
            anyLong())).thenReturn(Uni.createFrom().item(Optional.of(execution(ExecutionStatus.SUCCEEDED, 2L))));

        runtime.commit(record, "transition-1", result, itemContinuationHandler).await().indefinitely();

        InOrder inOrder = inOrder(terminalPublicationBarrier, executionStateStore);
        inOrder.verify(terminalPublicationBarrier).publishBeforeSuccess(record, result);
        inOrder.verify(executionStateStore).markSucceeded(
            eq(record.tenantId()),
            eq(record.executionId()),
            eq(record.version()),
            eq("transition-1"),
            eq(List.of("out-1")),
            anyLong());
    }

    @Test
    void publicationFailureDoesNotMarkSuccess() {
        ExecutionRecord<Object, Object> record = execution(ExecutionStatus.RUNNING, 1L);
        TransitionResultEnvelope result = TransitionResultEnvelope.completedInProcess(List.of("out-1"));
        IllegalStateException failure = new IllegalStateException("publish failed");
        when(terminalPublicationBarrier.publishBeforeSuccess(record, result))
            .thenReturn(Uni.createFrom().failure(failure));

        IllegalStateException thrown = assertThrows(
            IllegalStateException.class,
            () -> runtime.commit(record, "transition-1", result, itemContinuationHandler).await().indefinitely());

        assertEquals(failure, thrown);
        verify(executionStateStore, never()).markSucceeded(any(), any(), anyLong(), any(), any(), anyLong());
    }

    @Test
    void suspendedTransitionImportsWaitStateThenReleasesCompletedWork() {
        ExecutionRecord<Object, Object> record = execution(ExecutionStatus.RUNNING, 1L);
        ExecutionRecord<Object, Object> waiting = execution(ExecutionStatus.WAITING_EXTERNAL, 2L);
        TransitionAwaitSuspension suspension = new TransitionAwaitSuspension("tenant-1", "exec-1", "unit-1", 3);
        when(awaitCoordinator.importSuspension(suspension)).thenReturn(Uni.createFrom().voidItem());
        when(executionStateStore.markWaitingExternal(
            eq("tenant-1"),
            eq("exec-1"),
            eq(1L),
            eq("transition-1"),
            eq("unit-1"),
            eq(3),
            anyLong())).thenReturn(Uni.createFrom().item(Optional.of(waiting)));
        when(effects.releaseAlreadyCompletedAwaitUnit(
            eq(waiting),
            eq(suspension),
            anyLong(),
            eq(itemContinuationHandler))).thenReturn(Uni.createFrom().voidItem());

        runtime.commit(record, "transition-1", TransitionResultEnvelope.waiting(suspension), itemContinuationHandler)
            .await().indefinitely();

        InOrder inOrder = inOrder(awaitCoordinator, executionStateStore, effects);
        inOrder.verify(awaitCoordinator).importSuspension(suspension);
        inOrder.verify(executionStateStore).markWaitingExternal(
            eq("tenant-1"),
            eq("exec-1"),
            eq(1L),
            eq("transition-1"),
            eq("unit-1"),
            eq(3),
            anyLong());
        verify(effects).recordExecutionWaiting(any());
        verify(effects).releaseAlreadyCompletedAwaitUnit(
            eq(waiting),
            eq(suspension),
            anyLong(),
            eq(itemContinuationHandler));
    }

    @Test
    void suspendedTransitionDoesNotReleaseWorkWhenWaitStateWriteLosesRace() {
        ExecutionRecord<Object, Object> record = execution(ExecutionStatus.RUNNING, 1L);
        TransitionAwaitSuspension suspension = new TransitionAwaitSuspension("tenant-1", "exec-1", "unit-1", 3);
        when(awaitCoordinator.importSuspension(suspension)).thenReturn(Uni.createFrom().voidItem());
        when(executionStateStore.markWaitingExternal(
            eq("tenant-1"),
            eq("exec-1"),
            eq(1L),
            eq("transition-1"),
            eq("unit-1"),
            eq(3),
            anyLong())).thenReturn(Uni.createFrom().item(Optional.empty()));

        IllegalStateException failure = assertThrows(
            IllegalStateException.class,
            () -> runtime.commit(record, "transition-1", TransitionResultEnvelope.waiting(suspension), itemContinuationHandler)
                .await().indefinitely());

        assertEquals(
            "Failed to persist WAITING_EXTERNAL state for execution exec-1 at step 3 (expectedVersion=1, awaitUnitId=unit-1)",
            failure.getMessage());
        verify(effects, never()).recordExecutionWaiting(any());
        verify(effects, never()).releaseAlreadyCompletedAwaitUnit(any(), any(), anyLong(), any());
    }

    private static ExecutionRecord<Object, Object> execution(ExecutionStatus status, long version) {
        return new ExecutionRecord<>(
            "tenant-1",
            "exec-1",
            "exec-key",
            "pipeline",
            "contract",
            "release",
            ExecutionResultShape.MATERIALIZED_MULTI,
            status,
            version,
            3,
            0,
            "worker",
            0L,
            0L,
            "previous",
            "input",
            null,
            null,
            null,
            null,
            10_000L,
            10_000L,
            86_400L);
    }
}
