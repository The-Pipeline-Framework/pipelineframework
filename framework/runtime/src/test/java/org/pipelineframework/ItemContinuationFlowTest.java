package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.InMemoryExecutionStateStore;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.WorkDispatcher;

@ExtendWith(MockitoExtension.class)
class ItemContinuationFlowTest {

    @Mock
    private AwaitCoordinator awaitCoordinator;

    @Mock
    private ExecutionStateStore executionStateStore;

    @Mock
    private WorkDispatcher workDispatcher;

    @Mock
    private TransitionWorkerExecutor transitionWorkerExecutor;

    @Mock
    private AwaitItemContinuationHandler itemContinuationHandler;

    @Mock
    private ItemContinuationFlow.Effects effects;

    private CapturingScheduler scheduler;
    private AwaitItemContinuationHandler noopHandler;

    @BeforeEach
    void setUp() {
        scheduler = new CapturingScheduler();
        noopHandler = noopHandler();
    }

    @Test
    void readinessRequiresDispatchCompleteParentWaitingAndMatchingUnit() {
        ItemContinuationFlow runtime = runtime(executionStateStore, transitionWorkerExecutor);
        AwaitInteractionRecord record = itemRecord("exec-1", 0, AwaitInteractionStatus.COMPLETED, "approved");
        AwaitUnitRecord incompleteUnit = itemUnit(AwaitUnitStatus.WAITING_EXTERNAL, 1, 1, false);
        AwaitUnitRecord completeUnit = itemUnit(AwaitUnitStatus.COMPLETED, 1, 1, true);
        when(executionStateStore.getExecution("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(Optional.empty()))
            .thenReturn(Uni.createFrom().item(Optional.of(execution(ExecutionStatus.RUNNING, "other-unit"))))
            .thenReturn(Uni.createFrom().item(Optional.of(execution(ExecutionStatus.WAITING_EXTERNAL, "other-unit"))))
            .thenReturn(Uni.createFrom().item(Optional.of(execution(ExecutionStatus.WAITING_EXTERNAL, "unit-1"))));

        assertFalse(runtime.itemContinuationReady(null, completeUnit).await().indefinitely());
        assertFalse(runtime.itemContinuationReady(record, incompleteUnit).await().indefinitely());
        assertFalse(runtime.itemContinuationReady(record, completeUnit).await().indefinitely());
        assertFalse(runtime.itemContinuationReady(record, completeUnit).await().indefinitely());
        assertFalse(runtime.itemContinuationReady(record, completeUnit).await().indefinitely());
        assertTrue(runtime.itemContinuationReady(record, completeUnit).await().indefinitely());
    }

    @Test
    void saturatedAdmissionSchedulesRetryWithoutInvokingHandler() {
        ItemContinuationFlow runtime = runtime(executionStateStore, transitionWorkerExecutor);
        AwaitInteractionRecord record = itemRecord("exec-1", 0, AwaitInteractionStatus.COMPLETED, "approved");
        AwaitUnitRecord unit = itemUnit(AwaitUnitStatus.COMPLETED, 1, 1, true);
        when(transitionWorkerExecutor.tryAdmit()).thenReturn(Optional.empty());

        runtime.dispatchItemContinuation(record, unit, itemContinuationHandler, 42_000L);

        assertEquals(1, scheduler.scheduled.size());
        assertEquals(250L, scheduler.scheduled.getFirst().delayMs());
        verify(itemContinuationHandler, never()).continueAwaitItem(any(), any(), anyInt(), any(), anyLong());
    }

    @Test
    void saturatedAdmissionStopsAfterMaxAttemptsAndMarksParentFailed() {
        ItemContinuationFlow runtime = runtime(executionStateStore, transitionWorkerExecutor);
        AwaitInteractionRecord record = itemRecord("exec-1", 0, AwaitInteractionStatus.COMPLETED, "approved");
        AwaitUnitRecord unit = itemUnit(AwaitUnitStatus.COMPLETED, 1, 1, true);
        ExecutionRecord<Object, Object> waiting = execution(ExecutionStatus.WAITING_EXTERNAL, "unit-1");
        when(transitionWorkerExecutor.tryAdmit()).thenReturn(Optional.empty());
        when(executionStateStore.getExecution("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(Optional.of(waiting)));
        when(executionStateStore.markTerminalFailure(
                eq("tenant-1"),
                eq("exec-1"),
                eq(7L),
                eq(ExecutionStatus.FAILED),
                eq("await-item-continuation-failed:unit-1:0"),
                eq("AWAIT_ITEM_CONTINUATION_FAILED"),
                contains("admission remained saturated"),
                anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(waiting)));

        runtime.dispatchItemContinuation(record, unit, itemContinuationHandler, 42_000L);
        scheduler.runNext();
        scheduler.runNext();

        assertEquals(0, scheduler.scheduled.size());
        verify(executionStateStore).markTerminalFailure(
            eq("tenant-1"),
            eq("exec-1"),
            eq(7L),
            eq(ExecutionStatus.FAILED),
            eq("await-item-continuation-failed:unit-1:0"),
            eq("AWAIT_ITEM_CONTINUATION_FAILED"),
            contains("admission remained saturated"),
            anyLong());
    }

    @Test
    void synchronousContinuationFailureReleasesAdmissionPermit() {
        TransitionWorkerExecutor realExecutor = transitionExecutor(1);
        ItemContinuationFlow runtime = runtime(executionStateStore, realExecutor);
        AwaitInteractionRecord record = itemRecord("exec-1", 0, AwaitInteractionStatus.COMPLETED, "approved");
        AwaitUnitRecord unit = itemUnit(AwaitUnitStatus.COMPLETED, 1, 1, true);
        when(executionStateStore.getExecution("tenant-1", "exec-1"))
            .thenThrow(new IllegalStateException("store blew up"));

        runtime.dispatchItemContinuation(record, unit, itemContinuationHandler, 42_000L);

        assertEquals(0, realExecutor.activePermits());
        assertEquals(1, scheduler.scheduled.size());
    }

    @Test
    void continuationFailureRetriesThenMarksParentFailed() {
        TransitionWorkerExecutor realExecutor = transitionExecutor(1);
        ItemContinuationFlow runtime = runtime(executionStateStore, realExecutor);
        AwaitInteractionRecord record = itemRecord("exec-1", 0, AwaitInteractionStatus.COMPLETED, "approved");
        AwaitUnitRecord unit = itemUnit(AwaitUnitStatus.COMPLETED, 1, 1, true);
        ExecutionRecord<Object, Object> waiting = execution(ExecutionStatus.WAITING_EXTERNAL, "unit-1");
        when(executionStateStore.getExecution("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(Optional.of(waiting)));
        when(itemContinuationHandler.continueAwaitItem(
                eq(record),
                eq(unit),
                eq(3),
                eq(Optional.of(waiting)),
                eq(42_000L)))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("segment failed")));
        when(executionStateStore.markTerminalFailure(
                eq("tenant-1"),
                eq("exec-1"),
                eq(7L),
                eq(ExecutionStatus.FAILED),
                eq("await-item-continuation-failed:unit-1:0"),
                eq("AWAIT_ITEM_CONTINUATION_FAILED"),
                contains("segment failed"),
                anyLong()))
            .thenReturn(Uni.createFrom().item(Optional.of(waiting)));

        runtime.dispatchItemContinuation(record, unit, itemContinuationHandler, 42_000L);
        scheduler.runNext();
        scheduler.runNext();

        verify(itemContinuationHandler, times(3)).continueAwaitItem(
            eq(record),
            eq(unit),
            eq(3),
            eq(Optional.of(waiting)),
            eq(42_000L));
        verify(executionStateStore).markTerminalFailure(
            eq("tenant-1"),
            eq("exec-1"),
            eq(7L),
            eq(ExecutionStatus.FAILED),
            eq("await-item-continuation-failed:unit-1:0"),
            eq("AWAIT_ITEM_CONTINUATION_FAILED"),
            contains("segment failed"),
            anyLong());
    }

    @Test
    void earlyCompletedItemsDispatchAfterParentIsWaitingExternal() {
        TransitionWorkerExecutor realExecutor = transitionExecutor(4);
        ItemContinuationFlow runtime = runtime(executionStateStore, realExecutor);
        ExecutionRecord<Object, Object> waiting = execution(ExecutionStatus.WAITING_EXTERNAL, "unit-1");
        AwaitUnitRecord unit = itemUnit(AwaitUnitStatus.COMPLETED, 2, 2, true);
        AwaitInteractionRecord first = itemRecord("exec-1", 0, AwaitInteractionStatus.COMPLETED, "approved-0");
        AwaitInteractionRecord second = itemRecord("exec-1", 1, AwaitInteractionStatus.COMPLETED, "approved-1");
        TransitionAwaitSuspension suspension = new TransitionAwaitSuspension("tenant-1", "exec-1", "unit-1", 2);
        when(awaitCoordinator.getUnit("tenant-1", "unit-1")).thenReturn(Uni.createFrom().item(unit));
        when(awaitCoordinator.findByUnit("tenant-1", "unit-1")).thenReturn(Uni.createFrom().item(List.of(first, second)));
        when(executionStateStore.getExecution("tenant-1", "exec-1"))
            .thenReturn(Uni.createFrom().item(Optional.of(waiting)));
        when(itemContinuationHandler.continueAwaitItem(
                any(),
                eq(unit),
                eq(3),
                eq(Optional.of(waiting)),
                eq(42_000L)))
            .thenReturn(Uni.createFrom().voidItem());
        when(itemContinuationHandler.releaseAwaitParentIfReady(waiting, unit, 3, 42_000L))
            .thenReturn(Uni.createFrom().voidItem());

        runtime.releaseAlreadyCompletedAwaitUnit(waiting, suspension, 42_000L, itemContinuationHandler)
            .await().indefinitely();

        verify(itemContinuationHandler, times(2)).continueAwaitItem(
            any(),
            eq(unit),
            eq(3),
            eq(Optional.of(waiting)),
            eq(42_000L));
        verify(itemContinuationHandler).releaseAwaitParentIfReady(waiting, unit, 3, 42_000L);
    }

    @Test
    void childContinuationRecordingIsIdempotentAndReleasesParentInOrder() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        ItemContinuationFlow runtime = runtime(store, transitionWorkerExecutor);
        when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());
        long now = System.currentTimeMillis();
        CreateExecutionResult parent = store.createOrGetExecution(new ExecutionCreateCommand(
                "tenant-1",
                "parent-key",
                new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input"),
                ExecutionResultShape.MATERIALIZED_MULTI,
                now,
                now + 100_000))
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
        AwaitUnitRecord unit = itemUnit(AwaitUnitStatus.COMPLETED, 2, 2, true);

        runtime.recordAwaitItemContinuation(
                itemRecord(parent.record().executionId(), 0, AwaitInteractionStatus.COMPLETED, "first"),
                unit,
                4,
                new ExecutionInputSnapshot(ExecutionInputShape.UNI, "first-normalized"),
                List.of("out-0"),
                now)
            .await().indefinitely();
        runtime.recordAwaitItemContinuation(
                itemRecord(parent.record().executionId(), 0, AwaitInteractionStatus.COMPLETED, "first-duplicate"),
                unit,
                4,
                new ExecutionInputSnapshot(ExecutionInputShape.UNI, "duplicate-normalized"),
                List.of("duplicate-output"),
                now)
            .await().indefinitely();
        runtime.recordAwaitItemContinuation(
                itemRecord(parent.record().executionId(), 1, AwaitInteractionStatus.COMPLETED, "second"),
                unit,
                4,
                new ExecutionInputSnapshot(ExecutionInputShape.UNI, "second-normalized"),
                List.of("out-1"),
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
        ExecutionInputSnapshot firstChildInput = (ExecutionInputSnapshot) firstChild.inputPayload();
        assertEquals("first-normalized", firstChildInput.payload());
        verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-1", parent.record().executionId()));
    }

    private ItemContinuationFlow runtime(
        ExecutionStateStore store,
        TransitionWorkerExecutor executor) {
        return new ItemContinuationFlow(
            awaitCoordinator,
            store,
            workDispatcher,
            executor,
            scheduler,
            Runnable::run,
            () -> Duration.ofMillis(250),
            noopHandler,
            (record, unit, suspended, nowEpochMs) -> Uni.createFrom().voidItem(),
            effects);
    }

    private static AwaitItemContinuationHandler noopHandler() {
        return new AwaitItemContinuationHandler() {
            @Override
            public Uni<Void> continueAwaitItem(
                AwaitInteractionRecord record,
                AwaitUnitRecord unit,
                int nextStepIndex,
                Optional<ExecutionRecord<Object, Object>> parent,
                long nowEpochMs) {
                return Uni.createFrom().voidItem();
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
    }

    private static TransitionWorkerExecutor transitionExecutor(int maxInFlight) {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.WorkerConfig worker = mock(PipelineOrchestratorConfig.WorkerConfig.class);
        when(config.worker()).thenReturn(worker);
        when(worker.maxInFlight()).thenReturn(maxInFlight);
        return new TransitionWorkerExecutor(config);
    }

    private static AwaitInteractionRecord itemRecord(
        String executionId,
        int itemIndex,
        AwaitInteractionStatus status,
        Object responsePayload) {
        return new AwaitInteractionRecord(
            "tenant-1",
            executionId,
            "await",
            2,
            String.class.getName(),
            "interaction-" + itemIndex,
            "correlation-" + itemIndex,
            "causation-" + itemIndex,
            "idem-" + itemIndex,
            0L,
            status,
            "request-" + itemIndex,
            responsePayload,
            "unit-1",
            itemIndex,
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
        Integer expectedCount,
        int completedCount,
        boolean dispatchComplete) {
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

    private static ExecutionRecord<Object, Object> execution(ExecutionStatus status, String awaitUnitId) {
        return new ExecutionRecord<>(
            "tenant-1",
            "exec-1",
            "parent-key",
            "pipeline",
            "contract",
            "release",
            ExecutionResultShape.MATERIALIZED_MULTI,
            status,
            7L,
            2,
            0,
            "worker",
            0L,
            0L,
            "previous",
            "input",
            awaitUnitId,
            null,
            null,
            null,
            10_000L,
            10_000L,
            86_400L);
    }

    private static Set<String> completedKeys(int completedCount) {
        return IntStream.range(0, completedCount)
            .mapToObj(index -> "item:" + index)
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    private static final class CapturingScheduler implements ItemContinuationFlow.Scheduler {
        private final List<ScheduledTask> scheduled = new ArrayList<>();

        @Override
        public void schedule(Runnable task, long delayMs) {
            scheduled.add(new ScheduledTask(task, delayMs));
        }

        void runNext() {
            scheduled.removeFirst().task().run();
        }
    }

    private record ScheduledTask(Runnable task, long delayMs) {
    }
}
