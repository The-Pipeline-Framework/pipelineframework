package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCompletionTenantMismatchException;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitLiveCompletionRegistry;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;

@ExtendWith(MockitoExtension.class)
class AwaitCompletionFlowTest {

    @Mock
    private AwaitCoordinator awaitCoordinator;

    @Mock
    private AwaitLiveCompletionRegistry liveCompletionRegistry;

    @Mock
    private AwaitCompletionFlow.Effects effects;

    @Mock
    private AwaitItemContinuationHandler itemContinuationHandler;

    private AwaitCompletionFlow flow;

    @BeforeEach
    void setUp() {
        flow = new AwaitCompletionFlow(awaitCoordinator, liveCompletionRegistry, effects);
    }

    @Test
    void tenantMismatchStopsBeforeRecordingCompletion() {
        AwaitCompletionCommand command = command();
        AwaitInteractionRecord record = itemRecord("tenant-2");
        when(awaitCoordinator.complete(command)).thenReturn(Uni.createFrom().item(new AwaitCompletionResult(record, false)));

        AwaitCompletionTenantMismatchException failure = assertThrows(
            AwaitCompletionTenantMismatchException.class,
            () -> flow.complete(new QueueAsyncCommand.CompleteAwait(command), itemContinuationHandler)
                .await().indefinitely());

        assertEquals(
            "Await completion tenant mismatch: command tenant=tenant-1, record tenant=tenant-2",
            failure.getMessage());
        verify(awaitCoordinator, never()).recordCompletion(any(), anyLong());
        verify(liveCompletionRegistry, never()).signal(any(), any());
    }

    @Test
    void liveSignalFailureFallsBackToDurableItemContinuation() {
        AwaitCompletionCommand command = command();
        AwaitInteractionRecord record = itemRecord();
        AwaitUnitRecord unit = itemUnit(AwaitUnitStatus.COMPLETED, true, 1, 1);
        when(awaitCoordinator.complete(command)).thenReturn(Uni.createFrom().item(new AwaitCompletionResult(record, false)));
        when(awaitCoordinator.recordCompletion(record, command.nowEpochMs())).thenReturn(Uni.createFrom().item(unit));
        when(liveCompletionRegistry.signal(record, unit))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("live registry unavailable")));
        when(effects.itemContinuationReady(record, unit)).thenReturn(Uni.createFrom().item(true));

        flow.complete(new QueueAsyncCommand.CompleteAwait(command), itemContinuationHandler).await().indefinitely();

        verify(effects).dispatchItemContinuation(record, unit, itemContinuationHandler, command.nowEpochMs());
    }

    @Test
    void liveAdmissionBypassesDurableItemContinuation() {
        AwaitCompletionCommand command = command();
        AwaitInteractionRecord record = itemRecord();
        AwaitUnitRecord unit = itemUnit(AwaitUnitStatus.COMPLETED, true, 1, 1);
        when(awaitCoordinator.complete(command)).thenReturn(Uni.createFrom().item(new AwaitCompletionResult(record, false)));
        when(awaitCoordinator.recordCompletion(record, command.nowEpochMs())).thenReturn(Uni.createFrom().item(unit));
        when(liveCompletionRegistry.signal(record, unit)).thenReturn(Uni.createFrom().item(true));

        flow.complete(new QueueAsyncCommand.CompleteAwait(command), itemContinuationHandler).await().indefinitely();

        verify(liveCompletionRegistry).signal(record, unit);
        verify(effects, never()).itemContinuationReady(any(), any());
        verify(effects, never()).dispatchItemContinuation(any(), any(), any(), anyLong());
    }

    @Test
    void durableFallbackDispatchesReadyItemContinuation() {
        AwaitCompletionCommand command = command();
        AwaitInteractionRecord record = itemRecord();
        AwaitUnitRecord unit = itemUnit(AwaitUnitStatus.COMPLETED, true, 1, 1);
        when(awaitCoordinator.complete(command)).thenReturn(Uni.createFrom().item(new AwaitCompletionResult(record, false)));
        when(awaitCoordinator.recordCompletion(record, command.nowEpochMs())).thenReturn(Uni.createFrom().item(unit));
        when(liveCompletionRegistry.signal(record, unit)).thenReturn(Uni.createFrom().item(false));
        when(effects.itemContinuationReady(record, unit)).thenReturn(Uni.createFrom().item(true));

        flow.complete(new QueueAsyncCommand.CompleteAwait(command), itemContinuationHandler).await().indefinitely();

        verify(effects).dispatchItemContinuation(record, unit, itemContinuationHandler, command.nowEpochMs());
        verify(effects, never()).recordEarlyCompletionHeld(any(), any());
    }

    @Test
    void durableFallbackRecordsEarlyItemCompletionWhenParentGateIsClosed() {
        AwaitCompletionCommand command = command();
        AwaitInteractionRecord record = itemRecord();
        AwaitUnitRecord unit = itemUnit(AwaitUnitStatus.WAITING_EXTERNAL, true, 2, 1);
        when(awaitCoordinator.complete(command)).thenReturn(Uni.createFrom().item(new AwaitCompletionResult(record, false)));
        when(awaitCoordinator.recordCompletion(record, command.nowEpochMs())).thenReturn(Uni.createFrom().item(unit));
        when(liveCompletionRegistry.signal(record, unit)).thenReturn(Uni.createFrom().item(false));
        when(effects.itemContinuationReady(record, unit)).thenReturn(Uni.createFrom().item(false));

        flow.complete(new QueueAsyncCommand.CompleteAwait(command), itemContinuationHandler).await().indefinitely();

        verify(effects).recordEarlyCompletionHeld(record, unit);
        verify(effects, never()).dispatchItemContinuation(any(), any(), any(), anyLong());
    }

    @Test
    void itemCompletionOutcomeDispatchesOnlyWhenParentGateIsReady() {
        AwaitInteractionRecord record = itemRecord();
        AwaitUnitRecord unit = itemUnit(AwaitUnitStatus.COMPLETED, true, 1, 1);

        AwaitCompletionOutcome outcome = AwaitCompletionFlow.itemCompletionOutcome(record, unit, true, 42_000L);

        AwaitCompletionOutcome.DispatchItemContinuation dispatch =
            assertInstanceOf(AwaitCompletionOutcome.DispatchItemContinuation.class, outcome);
        assertEquals(record, dispatch.record());
        assertEquals(unit, dispatch.unit());
        assertEquals(42_000L, dispatch.nowEpochMs());
    }

    @Test
    void itemCompletionOutcomeRecordsOnlyWhenParentGateIsClosed() {
        AwaitCompletionOutcome outcome = AwaitCompletionFlow.itemCompletionOutcome(
            itemRecord(),
            itemUnit(AwaitUnitStatus.COMPLETED, true, 1, 1),
            false,
            42_000L);

        AwaitCompletionOutcome.RecordOnly recordOnly =
            assertInstanceOf(AwaitCompletionOutcome.RecordOnly.class, outcome);
        assertEquals("itemized-await-parent-not-ready", recordOnly.reason());
    }

    @Test
    void aggregateCompletionOutcomeReleasesOnlyCompletedUnit() {
        AwaitInteractionRecord record = itemRecord();
        AwaitUnitRecord unit = itemUnit(AwaitUnitStatus.COMPLETED, true, 1, 1);

        AwaitCompletionOutcome completed = AwaitCompletionFlow.aggregateCompletionOutcome(record, unit, 42_000L);
        AwaitCompletionOutcome.ReleaseParent release =
            assertInstanceOf(AwaitCompletionOutcome.ReleaseParent.class, completed);
        assertEquals(record, release.record());
        assertEquals(unit, release.unit());
        assertEquals(42_000L, release.nowEpochMs());

        AwaitCompletionOutcome waiting = AwaitCompletionFlow.aggregateCompletionOutcome(
            record,
            itemUnit(AwaitUnitStatus.WAITING_EXTERNAL, true, 2, 1),
            42_000L);
        AwaitCompletionOutcome.RecordOnly recordOnly =
            assertInstanceOf(AwaitCompletionOutcome.RecordOnly.class, waiting);
        assertEquals("await-unit-not-complete", recordOnly.reason());
    }

    private static AwaitCompletionCommand command() {
        return new AwaitCompletionCommand(
            "tenant-1",
            "interaction-0",
            "correlation-0",
            null,
            "idem-0",
            "approved",
            "tester",
            42_000L);
    }

    private static AwaitInteractionRecord itemRecord() {
        return itemRecord("tenant-1");
    }

    private static AwaitInteractionRecord itemRecord(String tenantId) {
        return new AwaitInteractionRecord(
            tenantId,
            "exec-1",
            "await",
            2,
            String.class.getName(),
            "interaction-0",
            "correlation-0",
            "causation-0",
            "idem-0",
            0L,
            AwaitInteractionStatus.COMPLETED,
            "request",
            "approved",
            "unit-1",
            0,
            null,
            null,
            null,
            "kafka",
            Map.of(),
            50_000L,
            40_000L,
            42_000L,
            86_400L);
    }

    private static AwaitUnitRecord itemUnit(
        AwaitUnitStatus status,
        boolean dispatchComplete,
        Integer expectedCount,
        int completedCount) {
        return new AwaitUnitRecord(
            "tenant-1",
            "unit-1",
            "exec-1",
            "await",
            2,
            "ONE_TO_ONE",
            0L,
            status,
            null,
            expectedCount,
            completedCount,
            completedKeys(completedCount),
            dispatchComplete,
            40_000L,
            42_000L,
            86_400L);
    }

    private static Set<String> completedKeys(int completedCount) {
        return IntStream.range(0, completedCount)
            .mapToObj(index -> "item:" + index)
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }
}
