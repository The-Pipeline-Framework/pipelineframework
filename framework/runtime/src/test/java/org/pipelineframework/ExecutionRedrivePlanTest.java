package org.pipelineframework;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ExecutionRedrivePlanTest {

  @Test
  void dlqRedriveUsesCurrentVersionAndDefaultReason() {
    ExecutionRecord<Object, Object> record = record(ExecutionStatus.DLQ, 4L);

    ExecutionRedrivePlan plan = ExecutionRedrivePlan.from(record, null, false, " ");

    assertEquals(4L, plan.expectedVersion());
    assertEquals("operator", plan.normalizedReason());
    assertEquals("redrive:exec-1:4", plan.transitionKey());
  }

  @Test
  void failedExecutionRequiresAllowFailed() {
    ExecutionRecord<Object, Object> record = record(ExecutionStatus.FAILED, 2L);

    IllegalStateException error = assertThrows(
        IllegalStateException.class,
        () -> ExecutionRedrivePlan.from(record, null, false, "retry"));

    assertEquals("Execution exec-1 cannot be re-driven from status FAILED", error.getMessage());
  }

  @Test
  void staleExpectedVersionIsRejectedBeforeEffects() {
    ExecutionRecord<Object, Object> record = record(ExecutionStatus.DLQ, 7L);

    IllegalStateException error = assertThrows(
        IllegalStateException.class,
        () -> ExecutionRedrivePlan.from(record, 6L, false, "retry"));

    assertEquals("Execution exec-1 version mismatch: expected 6 but current version is 7", error.getMessage());
  }

  @Test
  void reasonIsTrimmedAndCappedForDeterministicTransitionKey() {
    ExecutionRecord<Object, Object> record = record(ExecutionStatus.DLQ, 3L);
    String longReason = "  " + "x".repeat(90) + "  ";

    ExecutionRedrivePlan plan = ExecutionRedrivePlan.from(record, null, false, longReason);

    assertEquals("x".repeat(80), plan.normalizedReason());
    assertEquals("redrive:exec-1:3", plan.transitionKey());
  }

  private static ExecutionRecord<Object, Object> record(ExecutionStatus status, long version) {
    return new ExecutionRecord<>(
        "tenant-1",
        "exec-1",
        "key-1",
        "pipeline-a",
        "contract-a",
        "release-a",
        ExecutionResultShape.SINGLE,
        status,
        version,
        2,
        1,
        null,
        0L,
        0L,
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
