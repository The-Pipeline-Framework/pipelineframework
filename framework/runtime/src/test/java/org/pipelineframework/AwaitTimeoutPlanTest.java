package org.pipelineframework;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AwaitTimeoutPlanTest {

  @Test
  void missingParentRecordsTimeoutOnly() {
    AwaitTimeoutPlan plan = AwaitTimeoutPlan.from(interaction("unit-1"), Optional.empty());

    assertFalse(plan.failParent());
    assertTrue(plan.transitionKey().isEmpty());
  }

  @Test
  void terminalParentRecordsTimeoutOnly() {
    AwaitTimeoutPlan plan = AwaitTimeoutPlan.from(
        interaction("unit-1"),
        Optional.of(parent(ExecutionStatus.SUCCEEDED, "unit-1")));

    assertFalse(plan.failParent());
  }

  @Test
  void nonWaitingParentRecordsTimeoutOnly() {
    AwaitTimeoutPlan plan = AwaitTimeoutPlan.from(
        interaction("unit-1"),
        Optional.of(parent(ExecutionStatus.RUNNING, "unit-1")));

    assertFalse(plan.failParent());
  }

  @Test
  void differentAwaitUnitRecordsTimeoutOnly() {
    AwaitTimeoutPlan plan = AwaitTimeoutPlan.from(
        interaction("unit-1"),
        Optional.of(parent(ExecutionStatus.WAITING_EXTERNAL, "other-unit")));

    assertFalse(plan.failParent());
  }

  @Test
  void matchingWaitingParentFailsRun() {
    AwaitTimeoutPlan plan = AwaitTimeoutPlan.from(
        interaction("unit-1"),
        Optional.of(parent(ExecutionStatus.WAITING_EXTERNAL, "unit-1")));

    assertTrue(plan.failParent());
    assertEquals("exec-1:2:3", plan.transitionKey().orElseThrow());
    assertEquals("AWAIT_TIMEOUT", plan.errorCode());
    assertEquals("Await interaction timed out: interaction-1", plan.errorMessage());
  }

  @Test
  void constructorRejectsInvalidFailParentPlan() {
    assertThrows(IllegalArgumentException.class, () -> new AwaitTimeoutPlan(
        interaction("unit-1"),
        Optional.of(parent(ExecutionStatus.RUNNING, "unit-1")),
        true,
        Optional.of("exec-1:2:3"),
        "AWAIT_TIMEOUT",
        "Await interaction timed out: interaction-1"));
  }

  private static AwaitInteractionRecord interaction(String unitId) {
    return new AwaitInteractionRecord(
        "tenant-1",
        "exec-1",
        "AwaitStep",
        2,
        String.class.getName(),
        "interaction-1",
        "corr-1",
        "cause-1",
        "idem-1",
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
        1000L,
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
