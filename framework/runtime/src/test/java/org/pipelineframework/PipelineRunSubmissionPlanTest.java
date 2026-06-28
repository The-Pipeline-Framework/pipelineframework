package org.pipelineframework;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionResultShape;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineRunSubmissionPlanTest {

  @Test
  void scopesIdempotencyKeyByPipelineIdAndReleaseVersion() {
    PipelineRunSubmissionPlan left = plan("pipeline-a", "release-1", "client-key");
    PipelineRunSubmissionPlan right = plan("pipeline-b", "release-1", "client-key");

    assertEquals("10:pipeline-a:9:release-1:executionKey:client-key", left.executionKey());
    assertEquals("10:pipeline-b:9:release-1:executionKey:client-key", right.executionKey());
  }

  @Test
  void rejectsBlankScopedValues() {
    IllegalArgumentException pipeline = assertThrows(IllegalArgumentException.class,
        () -> plan(" ", "release-1", "client-key"));
    IllegalArgumentException release = assertThrows(IllegalArgumentException.class,
        () -> plan("pipeline-a", "", "client-key"));
    IllegalArgumentException executionKey = assertThrows(IllegalArgumentException.class,
        () -> plan("pipeline-a", "release-1", null));

    assertEquals("pipelineId must not be blank", pipeline.getMessage());
    assertEquals("releaseVersion must not be blank", release.getMessage());
    assertEquals("executionKey must not be blank", executionKey.getMessage());
  }

  @Test
  void buildsExecutionCreateCommandWithIdentityTtlSnapshotAndResultShape() {
    ExecutionInputSnapshot snapshot = new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input");
    PipelineRunSubmission submission = new PipelineRunSubmission(
        "tenant-1",
        true,
        "pipeline-a",
        "contract-a",
        "release-a",
        "client-key",
        false);
    PipelineRunSubmissionPlan plan = new PipelineRunSubmissionPlan(
        submission,
        snapshot,
        "resolved-key",
        ExecutionResultShape.MATERIALIZED_MULTI,
        100L,
        200L);

    ExecutionCreateCommand command = plan.createCommand();

    assertEquals("tenant-1", command.tenantId());
    assertEquals("10:pipeline-a:9:release-a:executionKey:resolved-key", command.executionKey());
    assertEquals("pipeline-a", command.pipelineId());
    assertEquals("contract-a", command.contractVersion());
    assertEquals("release-a", command.releaseVersion());
    assertEquals(snapshot, command.inputPayload());
    assertEquals(ExecutionResultShape.MATERIALIZED_MULTI, command.resultShape());
    assertEquals(100L, command.nowEpochMs());
    assertEquals(200L, command.ttlEpochS());
  }

  private PipelineRunSubmissionPlan plan(String pipelineId, String releaseVersion, String executionKey) {
    return new PipelineRunSubmissionPlan(
        new PipelineRunSubmission(
            "tenant-1",
            true,
            pipelineId,
            "contract-a",
            releaseVersion,
            "client-key",
            false),
        new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input"),
        executionKey,
        ExecutionResultShape.SINGLE,
        10L,
        20L);
  }
}
