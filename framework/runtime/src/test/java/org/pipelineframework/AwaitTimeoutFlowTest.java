package org.pipelineframework;

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
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AwaitTimeoutFlowTest {

  private AwaitTimeoutFlow flow;

  @Mock
  private AwaitCoordinator awaitCoordinator;

  @Mock
  private ExecutionStateStore executionStateStore;

  @Mock
  private SegmentBoundaryLedger segmentBoundaryLedger;

  @BeforeEach
  void setUp() {
    flow = new AwaitTimeoutFlow(awaitCoordinator, executionStateStore, () -> segmentBoundaryLedger);
  }

  @Test
  void timeoutRecordsFactBeforeFailingWaitingParent() {
    AwaitInteractionRecord earlier = interaction("unit-1", "interaction-a", "corr-a", 900L);
    AwaitInteractionRecord later = interaction("unit-1", "interaction-b", "corr-b", 950L);
    ExecutionRecord<Object, Object> parent = parent(ExecutionStatus.WAITING_EXTERNAL, "unit-1");
    ExecutionRecord<Object, Object> failed = parent(ExecutionStatus.FAILED, "unit-1");
    when(awaitCoordinator.findTimedOut(1000L, 100)).thenReturn(Uni.createFrom().item(List.of(later, earlier)));
    when(awaitCoordinator.markTimedOut(earlier, 1000L))
        .thenReturn(Uni.createFrom().item(Optional.of(earlier)));
    when(awaitCoordinator.markTimedOut(later, 1000L))
        .thenReturn(Uni.createFrom().item(Optional.of(later)));
    when(segmentBoundaryLedger.recordInteractionTimedOut(earlier, 1000L))
        .thenReturn(Uni.createFrom().voidItem());
    when(segmentBoundaryLedger.recordInteractionTimedOut(later, 1000L))
        .thenReturn(Uni.createFrom().voidItem());
    when(executionStateStore.getExecution("tenant-1", "exec-1"))
        .thenReturn(
            Uni.createFrom().item(Optional.of(parent)),
            Uni.createFrom().item(Optional.of(failed)));
    when(executionStateStore.markTerminalFailure(
            eq("tenant-1"),
            eq("exec-1"),
            eq(7L),
            eq(ExecutionStatus.FAILED),
            eq("exec-1:2:3"),
            eq("AWAIT_TIMEOUT"),
            eq("Await interaction timed out: interaction-a"),
            anyLong()))
        .thenReturn(Uni.createFrom().item(Optional.of(failed)));
    when(segmentBoundaryLedger.recordRunFailed(
            failed,
            "AWAIT_TIMEOUT",
            "Await interaction timed out: interaction-a",
            1000L))
        .thenReturn(Uni.createFrom().voidItem());

    flow.sweepTimedOut(1000L, 100).await().indefinitely();

    InOrder order = inOrder(awaitCoordinator, segmentBoundaryLedger, executionStateStore);
    order.verify(awaitCoordinator).markTimedOut(earlier, 1000L);
    order.verify(segmentBoundaryLedger).recordInteractionTimedOut(earlier, 1000L);
    order.verify(executionStateStore).getExecution("tenant-1", "exec-1");
    order.verify(executionStateStore).markTerminalFailure(
        eq("tenant-1"),
        eq("exec-1"),
        eq(7L),
        eq(ExecutionStatus.FAILED),
        eq("exec-1:2:3"),
        eq("AWAIT_TIMEOUT"),
        eq("Await interaction timed out: interaction-a"),
        anyLong());
    order.verify(segmentBoundaryLedger).recordRunFailed(
        failed,
        "AWAIT_TIMEOUT",
        "Await interaction timed out: interaction-a",
        1000L);
    order.verify(awaitCoordinator).markTimedOut(later, 1000L);
    order.verify(segmentBoundaryLedger).recordInteractionTimedOut(later, 1000L);
    order.verify(executionStateStore).getExecution("tenant-1", "exec-1");
  }

  @Test
  void missingParentDoesNotFailExecution() {
    AwaitInteractionRecord interaction = interaction("unit-1");
    when(awaitCoordinator.findTimedOut(1000L, 100)).thenReturn(Uni.createFrom().item(List.of(interaction)));
    when(awaitCoordinator.markTimedOut(interaction, 1000L))
        .thenReturn(Uni.createFrom().item(Optional.of(interaction)));
    when(segmentBoundaryLedger.recordInteractionTimedOut(interaction, 1000L))
        .thenReturn(Uni.createFrom().voidItem());
    when(executionStateStore.getExecution("tenant-1", "exec-1"))
        .thenReturn(Uni.createFrom().item(Optional.empty()));

    flow.sweepTimedOut(1000L, 100).await().indefinitely();

    verify(executionStateStore).getExecution("tenant-1", "exec-1");
    verify(segmentBoundaryLedger).recordInteractionTimedOut(interaction, 1000L);
    verify(executionStateStore, never()).markTerminalFailure(
        any(), any(), anyLong(), any(), any(), any(), any(), anyLong());
    verify(segmentBoundaryLedger, never()).recordRunFailed(any(), any(), any(), anyLong());
  }

  @Test
  void nonMatchingParentDoesNotFailExecution() {
    AwaitInteractionRecord interaction = interaction("unit-1");
    when(awaitCoordinator.findTimedOut(1000L, 100)).thenReturn(Uni.createFrom().item(List.of(interaction)));
    when(awaitCoordinator.markTimedOut(interaction, 1000L))
        .thenReturn(Uni.createFrom().item(Optional.of(interaction)));
    when(segmentBoundaryLedger.recordInteractionTimedOut(interaction, 1000L))
        .thenReturn(Uni.createFrom().voidItem());
    when(executionStateStore.getExecution("tenant-1", "exec-1"))
        .thenReturn(Uni.createFrom().item(Optional.of(parent(ExecutionStatus.WAITING_EXTERNAL, "other-unit"))));

    flow.sweepTimedOut(1000L, 100).await().indefinitely();

    verify(executionStateStore).getExecution("tenant-1", "exec-1");
    verify(segmentBoundaryLedger).recordInteractionTimedOut(interaction, 1000L);
    verify(executionStateStore, never()).markTerminalFailure(
        any(), any(), anyLong(), any(), any(), any(), any(), anyLong());
  }

  @Test
  void lostTimeoutAdmissionDoesNotLoadParent() {
    AwaitInteractionRecord interaction = interaction("unit-1");
    when(awaitCoordinator.findTimedOut(1000L, 100)).thenReturn(Uni.createFrom().item(List.of(interaction)));
    when(awaitCoordinator.markTimedOut(interaction, 1000L))
        .thenReturn(Uni.createFrom().item(Optional.empty()));

    flow.sweepTimedOut(1000L, 100).await().indefinitely();

    verify(executionStateStore, never()).getExecution(any(), any());
    verify(segmentBoundaryLedger, never()).recordInteractionTimedOut(any(), anyLong());
  }

  @Test
  void emptyTimeoutBatchIsNoOp() {
    when(awaitCoordinator.findTimedOut(1000L, 100)).thenReturn(Uni.createFrom().item(List.of()));

    flow.sweepTimedOut(1000L, 100).await().indefinitely();

    verify(awaitCoordinator, never()).markTimedOut(any(), anyLong());
    verify(executionStateStore, never()).getExecution(any(), any());
  }

  private static AwaitInteractionRecord interaction(String unitId) {
    return interaction(unitId, "interaction-1", "corr-1", 99999999L);
  }

  private static AwaitInteractionRecord interaction(
      String unitId,
      String interactionId,
      String correlationId,
      long deadlineEpochMs) {
    return new AwaitInteractionRecord(
        "tenant-1",
        "exec-1",
        "AwaitStep",
        2,
        String.class.getName(),
        interactionId,
        correlationId,
        "cause-" + interactionId,
        "idem-" + interactionId,
        1L,
        AwaitInteractionStatus.DISPATCHED,
        "request",
        null,
        unitId,
        null,
        "user-1",
        null,
        null,
        "kafka",
        java.util.Map.of(),
        deadlineEpochMs,
        1L,
        1L,
        99999999L);
  }

  private static ExecutionRecord<Object, Object> parent(ExecutionStatus status, String awaitUnitId) {
    return new ExecutionRecord<>(
        "tenant-1",
        "exec-1",
        "key-1",
        ExecutionResultShape.SINGLE,
        status,
        7L,
        2,
        3,
        null,
        0L,
        0L,
        null,
        "input",
        awaitUnitId,
        null,
        null,
        null,
        1L,
        1L,
        99999999L);
  }
}
