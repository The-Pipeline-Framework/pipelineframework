package org.pipelineframework;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AwaitContinuationPlannerTest {

  private final AwaitContinuationPlanner planner = new AwaitContinuationPlanner();

  @Test
  void scalarCompletedUnitPlansReleaseScalar() {
    AwaitInteractionRecord interaction = scalarRecord();
    AwaitUnitRecord unit = unit(AwaitUnitStatus.COMPLETED, null, 0, false, "interaction-1");

    AwaitContinuationPlan plan = planner.afterRecordedCompletion(interaction, unit, Optional.empty());

    AwaitContinuationPlan.ReleaseScalar release = assertInstanceOf(AwaitContinuationPlan.ReleaseScalar.class, plan);
    assertEquals(3, release.nextStepIndex());
  }

  @Test
  void itemizedCompletionBeforeParentWaitingPlansHoldCompletion() {
    AwaitInteractionRecord interaction = itemRecord(0);
    AwaitUnitRecord unit = unit(AwaitUnitStatus.COMPLETED, 1, 1, true, null);
    ExecutionRecord<Object, Object> parent = parent(ExecutionStatus.RUNNING);

    AwaitContinuationPlan plan = planner.afterRecordedCompletion(interaction, unit, Optional.of(parent));

    assertInstanceOf(AwaitContinuationPlan.HoldCompletion.class, plan);
  }

  @Test
  void readyItemizedParentPlansDispatchItemContinuations() {
    AwaitInteractionRecord interaction = itemRecord(0);
    AwaitUnitRecord unit = unit(AwaitUnitStatus.COMPLETED, 1, 1, true, null);
    ExecutionRecord<Object, Object> parent = parent(ExecutionStatus.WAITING_EXTERNAL);

    AwaitContinuationPlan plan = planner.afterRecordedCompletion(interaction, unit, Optional.of(parent));

    AwaitContinuationPlan.DispatchItemContinuations dispatch =
        assertInstanceOf(AwaitContinuationPlan.DispatchItemContinuations.class, plan);
    assertEquals(3, dispatch.nextStepIndex());
  }

  @Test
  void invalidItemIndexFailsDuringPlanning() {
    AwaitInteractionRecord interaction = itemRecord(3);
    AwaitUnitRecord unit = unit(AwaitUnitStatus.COMPLETED, 2, 1, true, null);

    IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () ->
        planner.recordItemOutput(
            parent(ExecutionStatus.WAITING_EXTERNAL),
            unit,
            interaction,
            4,
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input"),
            List.of("out")));

    assertEquals("Itemized await continuation itemIndex 3 is outside expected item count 2", error.getMessage());
  }

  @Test
  void allChildOutputsProduceOrderedParentRelease() {
    AwaitUnitRecord unit = unit(AwaitUnitStatus.COMPLETED, 2, 2, true, null);
    ExecutionRecord<Object, Object> parent = parent(ExecutionStatus.WAITING_EXTERNAL);
    ExecutionRecord<Object, Object> first = child("child-0", List.of("out-0"));
    ExecutionRecord<Object, Object> second = child("child-1", List.of("out-1"));

    AwaitContinuationPlan plan = planner.releaseItemizedParent(
        parent,
        unit,
        4,
        List.of(Optional.of(first), Optional.of(second)));

    AwaitContinuationPlan.ReleaseItemizedParent release =
        assertInstanceOf(AwaitContinuationPlan.ReleaseItemizedParent.class, plan);
    assertEquals(List.of("out-0", "out-1"), release.release().resumePayload().payload());
  }

  private static AwaitInteractionRecord scalarRecord() {
    return record(null);
  }

  private static AwaitInteractionRecord itemRecord(int itemIndex) {
    return record(itemIndex);
  }

  private static AwaitInteractionRecord record(Integer itemIndex) {
    return new AwaitInteractionRecord(
        "tenant-1",
        "exec-1",
        "AwaitPaymentProvider",
        2,
        String.class.getCanonicalName(),
        "interaction-" + (itemIndex == null ? "scalar" : itemIndex),
        "corr-" + (itemIndex == null ? "scalar" : itemIndex),
        "cause-1",
        "idem-" + (itemIndex == null ? "scalar" : itemIndex),
        1L,
        AwaitInteractionStatus.COMPLETED,
        Map.of("value", "request"),
        Map.of("value", "response"),
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

  private static AwaitUnitRecord unit(
      AwaitUnitStatus status,
      Integer expectedItemCount,
      int completedItemCount,
      boolean dispatchComplete,
      String primaryInteractionId) {
    return new AwaitUnitRecord(
        "tenant-1",
        "unit-1",
        "exec-1",
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

  private static ExecutionRecord<Object, Object> parent(ExecutionStatus status) {
    return new ExecutionRecord<>(
        "tenant-1",
        "exec-1",
        "key-1",
        ExecutionResultShape.MATERIALIZED_MULTI,
        status,
        7L,
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

  private static ExecutionRecord<Object, Object> child(String executionId, Object output) {
    return new ExecutionRecord<>(
        "tenant-1",
        executionId,
        "key-" + executionId,
        ExecutionResultShape.MATERIALIZED_MULTI,
        ExecutionStatus.SUCCEEDED,
        8L,
        4,
        0,
        null,
        0L,
        Long.MAX_VALUE,
        "transition",
        "input",
        null,
        output,
        null,
        null,
        1L,
        2L,
        999_999L);
  }
}
