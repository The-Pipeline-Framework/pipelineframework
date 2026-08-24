package org.pipelineframework;

import java.util.Objects;

import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionRedriveIntent;
import org.pipelineframework.orchestrator.ExecutionStatus;

record ExecutionRedrivePlan(
    ExecutionRecord<Object, Object> previous,
    long expectedVersion,
    boolean allowFailed,
    ExecutionRedriveIntent intent,
    String normalizedReason,
    String transitionKey) {

  ExecutionRedrivePlan {
    Objects.requireNonNull(previous, "previous must not be null");
    Objects.requireNonNull(intent, "intent must not be null");
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
    if (intent == ExecutionRedriveIntent.RETRY_FAILED_COMMAND
        && (previous.status() != ExecutionStatus.FAILED || !allowFailed)) {
      throw new IllegalStateException(
          "Deliberate Command retry requires an explicitly allowed FAILED execution");
    }
    if (intent == ExecutionRedriveIntent.RETRY_FAILED_COMMAND
        && previous.failedStepIndex() < previous.currentStepIndex()) {
      throw new IllegalStateException(
          "Execution " + previous.executionId()
              + " does not retain a failed Command step eligible for deliberate retry");
    }
  }

  static ExecutionRedrivePlan from(
      ExecutionRecord<Object, Object> previous,
      Long expectedVersion,
      boolean allowFailed,
      String reason) {
    return from(previous, expectedVersion, allowFailed, ExecutionRedriveIntent.REPLAY, reason);
  }

  static ExecutionRedrivePlan from(
      ExecutionRecord<Object, Object> previous,
      Long expectedVersion,
      boolean allowFailed,
      ExecutionRedriveIntent intent,
      String reason) {
    Objects.requireNonNull(previous, "previous must not be null");
    ExecutionRedriveIntent resolvedIntent = Objects.requireNonNull(intent, "intent must not be null");
    long version = expectedVersion == null ? previous.version() : expectedVersion;
    String normalizedReason = normalizeReason(reason);
    String transitionKey = resolvedIntent == ExecutionRedriveIntent.REPLAY
        ? "redrive:" + previous.executionId() + ":" + version
        : "command-retry:" + previous.executionId() + ":" + version;
    return new ExecutionRedrivePlan(
        previous,
        version,
        allowFailed,
        resolvedIntent,
        normalizedReason,
        transitionKey);
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
