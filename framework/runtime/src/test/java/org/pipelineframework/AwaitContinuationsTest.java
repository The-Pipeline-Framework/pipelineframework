package org.pipelineframework;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AwaitContinuationsTest {

  private InMemoryControlPlaneJournal journal;
  private AwaitContinuations continuations;
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
    continuations = continuations(executionStateStore, workDispatcher, awaitCoordinator);
  }

  @AfterEach
  void tearDown() {
    scheduler.shutdownNow();
  }

  @Test
  void earlyItemCompletionWaitsForParentWaitingExternal() {
    AwaitInteractionRecord interaction = itemAwaitRecord("exec-1", 0, AwaitInteractionStatus.COMPLETED, "approved");
    AwaitUnitRecord unit = awaitUnit(AwaitUnitStatus.COMPLETED, 1, 1, true, null);
    AwaitItemContinuationHandler handler = org.mockito.Mockito.mock(AwaitItemContinuationHandler.class);
    ExecutionRecord<Object, Object> runningParent = record("tenant-1", "exec-1", "key-1", ExecutionStatus.RUNNING, 7L);
    when(executionStateStore.getExecution("tenant-1", "exec-1"))
        .thenReturn(Uni.createFrom().item(java.util.Optional.of(runningParent)));

    continuations.afterRecordedCompletion(
            new org.pipelineframework.awaitable.AwaitCompletionResult(interaction, false),
            unit,
            handler,
            1234L)
        .await().indefinitely();

    verify(handler, never()).continueAwaitItem(any(), any(), any(Integer.class), any(), any(Long.class));
    verify(workDispatcher, never()).enqueueNow(any());
  }

  @Test
  void readyItemCompletionDispatchesAllCompletedItemsForUnit() {
    AwaitInteractionRecord held = itemAwaitRecord("exec-1", 0, AwaitInteractionStatus.COMPLETED, "first");
    AwaitInteractionRecord current = itemAwaitRecord("exec-1", 1, AwaitInteractionStatus.COMPLETED, "second");
    AwaitUnitRecord unit = awaitUnit(AwaitUnitStatus.COMPLETED, 2, 2, true, null);
    AwaitItemContinuationHandler handler = org.mockito.Mockito.mock(AwaitItemContinuationHandler.class);
    ExecutionRecord<Object, Object> waitingParent =
        record("tenant-1", "exec-1", "key-1", ExecutionStatus.WAITING_EXTERNAL, 7L);
    when(executionStateStore.getExecution("tenant-1", "exec-1"))
        .thenReturn(Uni.createFrom().item(java.util.Optional.of(waitingParent)));
    when(awaitCoordinator.findByUnit("tenant-1", "unit-1"))
        .thenReturn(Uni.createFrom().item(List.of(held, current)));
    when(handler.continueAwaitItem(any(), any(), any(Integer.class), any(), any(Long.class)))
        .thenReturn(Uni.createFrom().voidItem());

    continuations.afterRecordedCompletion(
            new org.pipelineframework.awaitable.AwaitCompletionResult(current, false),
            unit,
            handler,
            1234L)
        .await().indefinitely();

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
    continuations.afterRecordedCompletion(
            new org.pipelineframework.awaitable.AwaitCompletionResult(current, true),
            unit,
            handler,
            1235L)
        .await().indefinitely();
    verify(handler, org.mockito.Mockito.after(300).times(2)).continueAwaitItem(
        any(),
        eq(unit),
        eq(3),
        any(),
        any(Long.class));
  }

  @Test
  void captureItemContinuationOutputStoresChildrenAndReleasesParentInOrder() {
    InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
    continuations = continuations(store, workDispatcher, awaitCoordinator);
    when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());
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
    AwaitUnitRecord unit = awaitUnit(AwaitUnitStatus.COMPLETED, 2, 2, true, null);

    continuations.captureItemContinuationOutput(
            itemAwaitRecord(parent.record().executionId(), 1, AwaitInteractionStatus.COMPLETED, "second"),
            unit,
            4,
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "second-normalized"),
            List.of("out-1"),
            now)
        .await().indefinitely();
    verify(workDispatcher, never()).enqueueNow(any());

    continuations.captureItemContinuationOutput(
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
    assertInstanceOf(ExecutionInputSnapshot.class, resumed.inputPayload());
    ExecutionInputSnapshot snapshot = (ExecutionInputSnapshot) resumed.inputPayload();
    assertEquals(ExecutionInputShape.MULTI, snapshot.shape());
    assertEquals(List.of("out-0", "out-1"), snapshot.payload());
    verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-1", parent.record().executionId()));
    ControlPlaneProjection projection = journal.projection("tenant-1", parent.record().executionId())
        .await().indefinitely();
    assertTrue(projection.factKeys().contains("continuation-segment-created:"
        + parent.record().executionId() + ":segment:await-item:unit-1:0"));
    assertTrue(projection.factKeys().contains("continuation-segment-created:"
        + parent.record().executionId() + ":segment:await-item:unit-1:1"));
    assertTrue(projection.factKeys().contains("continuation-segment-created:"
        + parent.record().executionId() + ":segment:4"));
  }

  @Test
  void succeededChildRetryRecordsContinuationFactBeforeParentRelease() {
    InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
    continuations = continuations(store, workDispatcher, awaitCoordinator);
    when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());
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
    AwaitUnitRecord unit = awaitUnit(parent.record().executionId(), AwaitUnitStatus.COMPLETED, 1, 1, true, null);
    String childKey = "parent-key:await-item:unit-1:0";
    CreateExecutionResult child = store.createOrGetExecution(new ExecutionCreateCommand(
            "tenant-1",
            childKey,
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "normalized"),
            ExecutionResultShape.MATERIALIZED_MULTI,
            now,
            9_999_999_999L))
        .await().indefinitely();
    store.markSucceeded(
            "tenant-1",
            child.record().executionId(),
            child.record().version(),
            "already-succeeded",
            List.of("out-0"),
            now)
        .await().indefinitely();

    continuations.captureItemContinuationOutput(
            itemAwaitRecord(parent.record().executionId(), 0, AwaitInteractionStatus.COMPLETED, "first"),
            unit,
            4,
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "normalized"),
            List.of("out-0"),
            now)
        .await().indefinitely();

    ControlPlaneProjection projection = journal.projection("tenant-1", parent.record().executionId())
        .await().indefinitely();
    assertTrue(projection.factKeys().contains("continuation-segment-created:"
        + parent.record().executionId() + ":segment:await-item:unit-1:0"));
    assertTrue(projection.factKeys().contains("continuation-segment-created:"
        + parent.record().executionId() + ":segment:4"));
    verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-1", parent.record().executionId()));
  }

  @Test
  void alreadyReleasedParentIsReenqueuedOnContinuationReleaseRetry() {
    InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
    continuations = continuations(store, workDispatcher, awaitCoordinator);
    when(workDispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());
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
    AwaitUnitRecord unit = awaitUnit(parent.record().executionId(), AwaitUnitStatus.COMPLETED, 1, 1, true, null);
    String childKey = "parent-key:await-item:unit-1:0";
    CreateExecutionResult child = store.createOrGetExecution(new ExecutionCreateCommand(
            "tenant-1",
            childKey,
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "normalized"),
            ExecutionResultShape.MATERIALIZED_MULTI,
            now,
            9_999_999_999L))
        .await().indefinitely();
    store.markSucceeded(
            "tenant-1",
            child.record().executionId(),
            child.record().version(),
            "child-succeeded",
            List.of("out-0"),
            now)
        .await().indefinitely();
    store.markAwaitItemContinuationsCompleted(
            "tenant-1",
            parent.record().executionId(),
            "unit-1",
            4,
            new ExecutionInputSnapshot(ExecutionInputShape.MULTI, List.of("out-0")),
            now)
        .await().indefinitely();

    continuations.releaseParentIfReady(parent.record(), unit, 4, now)
        .await().indefinitely();

    ControlPlaneProjection projection = journal.projection("tenant-1", parent.record().executionId())
        .await().indefinitely();
    assertTrue(projection.factKeys().contains("continuation-segment-created:"
        + parent.record().executionId() + ":segment:4"));
    verify(workDispatcher).enqueueNow(new ExecutionWorkItem("tenant-1", parent.record().executionId()));
  }

  private AwaitContinuations continuations(
      ExecutionStateStore stateStore,
      WorkDispatcher dispatcher,
      AwaitCoordinator coordinator) {
    return new AwaitContinuations(
        stateStore,
        dispatcher,
        coordinator,
        new TransitionWorkerExecutor(null, new PipelineInvocationRuntime()),
        scheduler,
        () -> Duration.ofMillis(10),
        () -> new SegmentBoundaryLedger(journal),
        ignored -> {
        });
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
        AwaitUnitStatus status,
        Integer expectedItemCount,
        int completedItemCount,
        boolean dispatchComplete,
        String primaryInteractionId) {
      return awaitUnit("exec-1", status, expectedItemCount, completedItemCount, dispatchComplete, primaryInteractionId);
    }

    private static AwaitUnitRecord awaitUnit(
        String executionId,
        AwaitUnitStatus status,
        Integer expectedItemCount,
        int completedItemCount,
        boolean dispatchComplete,
        String primaryInteractionId) {
      return new AwaitUnitRecord(
          "tenant-1",
          "unit-1",
          executionId,
          "AwaitPaymentProvider",
        2,
        "ONE_TO_ONE",
        1L,
        status,
        primaryInteractionId,
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
      String tenantId,
      String executionId,
      String executionKey,
      ExecutionStatus status,
      long version) {
    return new ExecutionRecord<>(
        tenantId,
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
