package org.pipelineframework;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DueExecutionDispatchPlanTest {

  @Test
  void dueRecordsBecomeExecutionWorkItems() {
    DueExecutionDispatchPlan plan = DueExecutionDispatchPlan.from(List.of(
        record("tenant-a", "exec-a"),
        record("tenant-b", "exec-b")));

    assertFalse(plan.empty());
    assertEquals(List.of(
        new ExecutionWorkItem("tenant-a", "exec-a"),
        new ExecutionWorkItem("tenant-b", "exec-b")), plan.workItems());
  }

  @Test
  void dueRecordsAreSortedByDueTimeTenantAndExecutionId() {
    DueExecutionDispatchPlan plan = DueExecutionDispatchPlan.from(List.of(
        record("tenant-b", "exec-b", 20L),
        record("tenant-b", "exec-a", 10L),
        record("tenant-a", "exec-c", 10L)));

    assertEquals(List.of(
        new ExecutionWorkItem("tenant-a", "exec-c"),
        new ExecutionWorkItem("tenant-b", "exec-a"),
        new ExecutionWorkItem("tenant-b", "exec-b")), plan.workItems());
  }

  @Test
  void emptyDueListIsNoOpPlan() {
    DueExecutionDispatchPlan plan = DueExecutionDispatchPlan.from(List.of());

    assertTrue(plan.empty());
  }

  @Test
  void workItemListIsDefensivelyCopied() {
    List<ExecutionWorkItem> items = new java.util.ArrayList<>();
    items.add(new ExecutionWorkItem("tenant-1", "exec-1"));

    DueExecutionDispatchPlan plan = new DueExecutionDispatchPlan(items);
    items.add(new ExecutionWorkItem("tenant-2", "exec-2"));

    assertEquals(List.of(new ExecutionWorkItem("tenant-1", "exec-1")), plan.workItems());
    assertThrows(UnsupportedOperationException.class, () ->
        plan.workItems().add(new ExecutionWorkItem("tenant-3", "exec-3")));
  }

  private static ExecutionRecord<Object, Object> record(String tenantId, String executionId) {
    return record(tenantId, executionId, 1L);
  }

  private static ExecutionRecord<Object, Object> record(String tenantId, String executionId, long nextDueEpochMs) {
    return new ExecutionRecord<>(
        tenantId,
        executionId,
        executionId + "-key",
        ExecutionResultShape.SINGLE,
        ExecutionStatus.WAIT_RETRY,
        1L,
        2,
        1,
        null,
        0L,
        nextDueEpochMs,
        null,
        "input",
        null,
        null,
        null,
        null,
        1L,
        1L,
        99999999L);
  }
}
