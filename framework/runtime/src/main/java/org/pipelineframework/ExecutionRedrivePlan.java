package org.pipelineframework;

import java.util.Objects;

import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStatus;

record ExecutionRedrivePlan(
    ExecutionRecord<Object, Object> previous,
    long expectedVersion,
    boolean allowFailed,
    String normalizedReason,
    String transitionKey) {

  ExecutionRedrivePlan {
    Objects.requireNonNull(previous, "previous must not be null");
    Objects.requireNonNull(normalizedReason, "normalizedReason must not be null");
    Objects.requireNonNull(transitionKey, "transitionKey must not be null");
    if (!redrivable(previous.status(), allowFailed)) {
      throw new IllegalStateException(
          "Execution " + previous.executionId() + " cannot be re-driven from status " + previous.status());
    }
    if (expectedVersion != previous.version()) {
      throw new IllegalStateException(
          "Execution " + previous.executionId() + " version mismatch: expected " + expectedVersion
              + " but current version is " + previous.version());
    }
  }

  static ExecutionRedrivePlan from(
      ExecutionRecord<Object, Object> previous,
      Long expectedVersion,
      boolean allowFailed,
      String reason) {
    Objects.requireNonNull(previous, "previous must not be null");
    long version = expectedVersion == null ? previous.version() : expectedVersion;
    String normalizedReason = normalizeReason(reason);
    return new ExecutionRedrivePlan(
        previous,
        version,
        allowFailed,
        normalizedReason,
        "redrive:" + previous.executionId() + ":" + version);
  }

  private static boolean redrivable(ExecutionStatus status, boolean allowFailed) {
    return status == ExecutionStatus.DLQ || (allowFailed && status == ExecutionStatus.FAILED);
  }

  private static String normalizeReason(String reason) {
    if (reason == null || reason.isBlank()) {
      return "operator";
    }
    String trimmed = reason.trim();
    return trimmed.length() <= 80 ? trimmed : trimmed.substring(0, 80);
  }
}
