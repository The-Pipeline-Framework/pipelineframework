package org.pipelineframework;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
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
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.ControlPlaneProjection;
import org.pipelineframework.orchestrator.controlplane.InMemoryControlPlaneJournal;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ItemizedAwaitContinuationFlowTest {

  private InMemoryControlPlaneJournal journal;
  private ScheduledExecutorService scheduler;

  @Mock
  private ExecutionStateStore executionStateStore;

  @Mock
  private WorkDispatcher workDispatcher;

  @Mock
  private AwaitCoordinator awaitCoordinator;

  @BeforeEach
  void setUp() {
    journal = new InMemoryControlPlaneJournal();
    scheduler = Executors.newSingleThreadScheduledExecutor();
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  @Test
  void duplicateCompletedItemDoesNotDispatchTwice() {
    AwaitInteractionRecord completed = itemAwaitRecord("exec-1", 0, AwaitInteractionStatus.COMPLETED, "approved");
    AwaitUnitRecord unit = awaitUnit("exec-1", AwaitUnitStatus.COMPLETED, 1, 1, true);
    ExecutionRecord<Object, Object> parent = record("exec-1", "key-1", ExecutionStatus.WAITING_EXTERNAL, 7L);
    AwaitItemContinuationHandler handler = org.mockito.Mockito.mock(AwaitItemContinuationHandler.class);
    when(executionStateStore.getExecution("tenant-1", "exec-1"))
        .thenReturn(Uni.createFrom().item(Optional.of(parent)));
    when(awaitCoordinator.findByUnit("tenant-1", "unit-1"))
        .thenReturn(Uni.createFrom().item(List.of(completed)));
    when(handler.continueAwaitItem(eq(completed), eq(unit), eq(3), any(), any(Long.class)))
        .thenReturn(Uni.createFrom().voidItem());
    ItemizedAwaitContinuationFlow flow = flow(executionStateStore, workDispatcher, awaitCoordinator);

    flow.afterRecordedCompletion(completed, unit, handler, 1234L).await().indefinitely();
    verify(handler, timeout(1000).times(1)).continueAwaitItem(eq(completed), eq(unit), eq(3), any(), eq(1234L));

    flow.afterRecordedCompletion(completed, unit, handler, 1235L).await().indefinitely();
    verify(handler, after(300).times(1)).continueAwaitItem(eq(completed), eq(unit), eq(3), any(), any(Long.class));
  }

  @Test
  void childOutputsReleaseParentOnceWithOrderedOutput() {
    InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
    when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());
    ItemizedAwaitContinuationFlow flow = flow(store, workDispatcher, awaitCoordinator);
    long now = 1234L;
    CreateExecutionResult parent = store.createOrGetExecution(new ExecutionCreateCommand(
            "tenant-1",
            "parent-key",
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input"),
            ExecutionResultShape.MATERIALIZED_MULTI,
            now,
            9_999_999_999L))
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
    AwaitUnitRecord unit = awaitUnit(parent.record().executionId(), AwaitUnitStatus.COMPLETED, 2, 2, true);

    flow.captureOutput(
            itemAwaitRecord(parent.record().executionId(), 1, AwaitInteractionStatus.COMPLETED, "second"),
            unit,
            4,
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "second-normalized"),
            List.of("out-1"),
            now)
        .await().indefinitely();
    verify(workDispatcher, never()).enqueueNow(any());

    flow.captureOutput(
            itemAwaitRecord(parent.record().executionId(), 0, AwaitInteractionStatus.COMPLETED, "first"),
            unit,
            4,
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "first-normalized"),
            List.of("out-0"),
            now)
        .await().indefinitely();

    ExecutionRecord<Object, Object> resumed = store.getExecution("tenant-1", parent.record().executionId())
        .await().indefinitely()
        .orElseThrow();
    assertEquals(ExecutionStatus.QUEUED, resumed.status());
    assertEquals(4, resumed.currentStepIndex());
    assertNull(resumed.awaitUnitId());
    ExecutionInputSnapshot snapshot = assertInstanceOf(ExecutionInputSnapshot.class, resumed.inputPayload());
    assertEquals(List.of("out-0", "out-1"), snapshot.payload());
    verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-1", parent.record().executionId()));
    ControlPlaneProjection projection = journal.projection("tenant-1", parent.record().executionId())
        .await().indefinitely();
    assertTrue(projection.factKeys().contains("continuation-segment-created:"
        + parent.record().executionId() + ":segment:4"));
  }

  @Test
  void readyCompletionDispatchesAllCompletedItemsForUnit() {
    AwaitInteractionRecord first = itemAwaitRecord("exec-1", 0, AwaitInteractionStatus.COMPLETED, "first");
    AwaitInteractionRecord second = itemAwaitRecord("exec-1", 1, AwaitInteractionStatus.COMPLETED, "second");
    AwaitUnitRecord unit = awaitUnit("exec-1", AwaitUnitStatus.COMPLETED, 2, 2, true);
    ExecutionRecord<Object, Object> parent = record("exec-1", "key-1", ExecutionStatus.WAITING_EXTERNAL, 7L);
    AwaitItemContinuationHandler handler = org.mockito.Mockito.mock(AwaitItemContinuationHandler.class);
    when(executionStateStore.getExecution("tenant-1", "exec-1"))
        .thenReturn(Uni.createFrom().item(Optional.of(parent)));
    when(awaitCoordinator.findByUnit("tenant-1", "unit-1"))
        .thenReturn(Uni.createFrom().item(List.of(first, second)));
    when(handler.continueAwaitItem(any(), eq(unit), eq(3), any(), any(Long.class)))
        .thenReturn(Uni.createFrom().voidItem());
    ItemizedAwaitContinuationFlow flow = flow(executionStateStore, workDispatcher, awaitCoordinator);

    flow.afterRecordedCompletion(second, unit, handler, 1234L).await().indefinitely();

    ArgumentCaptor<AwaitInteractionRecord> captor = ArgumentCaptor.forClass(AwaitInteractionRecord.class);
    verify(handler, timeout(1000).times(2)).continueAwaitItem(
        captor.capture(),
        eq(unit),
        eq(3),
        any(),
        eq(1234L));
    assertEquals(Set.of(0, 1), new TreeSet<>(captor.getAllValues().stream()
        .map(AwaitInteractionRecord::itemIndex)
        .toList()));
  }

  private ItemizedAwaitContinuationFlow flow(
      ExecutionStateStore stateStore,
      WorkDispatcher dispatcher,
      AwaitCoordinator coordinator) {
    AwaitContinuationPlanner planner = new AwaitContinuationPlanner();
    return new ItemizedAwaitContinuationFlow(
        stateStore,
        dispatcher,
        coordinator,
        new TransitionWorkerExecutor(null, new PipelineInvocationRuntime()),
        scheduler,
        () -> Duration.ofMillis(10),
        () -> new SegmentBoundaryLedger(journal),
        ignored -> {
        },
        planner,
        new ItemContinuationClaims());
  }

  private static AwaitInteractionRecord itemAwaitRecord(
      String executionId,
      int itemIndex,
      AwaitInteractionStatus status,
      String response) {
    return new AwaitInteractionRecord(
        "tenant-1",
        executionId,
        "AwaitPaymentProvider",
        2,
        String.class.getCanonicalName(),
        "interaction-" + itemIndex,
        "corr-" + itemIndex,
        "cause-1",
        "idem-" + itemIndex,
        1L,
        status,
        Map.of("value", "request-" + itemIndex),
        Map.of("value", response),
        "unit-1",
        itemIndex,
        "user-1",
        null,
        null,
        "kafka",
        Map.of(),
        10_000L,
        1L,
        2L,
        999_999L);
  }

  private static AwaitUnitRecord awaitUnit(
      String executionId,
      AwaitUnitStatus status,
      Integer expectedItemCount,
      int completedItemCount,
      boolean dispatchComplete) {
    return new AwaitUnitRecord(
        "tenant-1",
        "unit-1",
        executionId,
        "AwaitPaymentProvider",
        2,
        "ONE_TO_ONE",
        1L,
        status,
        null,
        expectedItemCount,
        completedItemCount,
        completedItemCount == 0
            ? Set.of()
            : completedItemCount == 1 ? Set.of("item:0") : Set.of("item:0", "item:1"),
        dispatchComplete,
        1L,
        2L,
        999_999L);
  }

  private static ExecutionRecord<Object, Object> record(
      String executionId,
      String executionKey,
      ExecutionStatus status,
      long version) {
    return new ExecutionRecord<>(
        "tenant-1",
        executionId,
        executionKey,
        ExecutionResultShape.MATERIALIZED_MULTI,
        status,
        version,
        2,
        0,
        null,
        0L,
        Long.MAX_VALUE,
        "transition",
        "input",
        "unit-1",
        null,
        null,
        null,
        1L,
        2L,
        999_999L);
  }
}
