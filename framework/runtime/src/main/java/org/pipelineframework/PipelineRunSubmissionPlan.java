package org.pipelineframework;

import java.util.Objects;

import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionResultShape;

record PipelineRunSubmissionPlan(
    PipelineRunSubmission submission,
    ExecutionInputSnapshot snapshot,
    String executionKey,
    ExecutionResultShape resultShape,
    long nowEpochMs,
    long ttlEpochS) {

  PipelineRunSubmissionPlan {
    Objects.requireNonNull(submission, "submission must not be null");
    Objects.requireNonNull(snapshot, "snapshot must not be null");
    Objects.requireNonNull(resultShape, "resultShape must not be null");
    executionKey = scopedRootExecutionKey(
        submission.pipelineId(),
        submission.releaseVersion(),
        executionKey);
  }

  ExecutionCreateCommand createCommand() {
    return new ExecutionCreateCommand(
        submission.tenantId(),
        executionKey,
        submission.pipelineId(),
        submission.contractVersion(),
        submission.releaseVersion(),
        snapshot,
        resultShape,
        nowEpochMs,
        ttlEpochS);
  }

  private static String scopedRootExecutionKey(String pipelineId, String releaseVersion, String executionKey) {
    return compositeScopedKey("pipelineId", pipelineId, "releaseVersion", releaseVersion)
        + ":executionKey:"
        + requireScopedValue("executionKey", executionKey);
  }

  private static String compositeScopedKey(String leftName, String left, String rightName, String right) {
    String safeLeft = requireScopedValue(leftName, left);
    String safeRight = requireScopedValue(rightName, right);
    return safeLeft.length() + ":" + safeLeft + ":" + safeRight.length() + ":" + safeRight;
  }

  private static String requireScopedValue(String name, String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
