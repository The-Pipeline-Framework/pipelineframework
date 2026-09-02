package org.pipelineframework;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionRedriveIntent;
import org.pipelineframework.orchestrator.ExecutionStatus;

record ExecutionRedrivePlan(
    ExecutionRecord<Object, Object> previous,
    long expectedVersion,
    boolean allowFailed,
    ExecutionRedriveIntent intent,
    Optional<String> targetCommandId,
    String normalizedReason,
    String transitionKey) {

  ExecutionRedrivePlan {
    Objects.requireNonNull(previous, "previous must not be null");
    Objects.requireNonNull(intent, "intent must not be null");
    targetCommandId = Optional.ofNullable(targetCommandId).orElseGet(Optional::empty);
    Objects.requireNonNull(normalizedReason, "normalizedReason must not be null");
    Objects.requireNonNull(transitionKey, "transitionKey must not be null");
    if (!redrivable(previous.status(), allowFailed, intent)) {
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
    if (intent == ExecutionRedriveIntent.RETRY_FAILED_COMMAND
        && previous.failedCommandId().filter(value -> !value.isBlank()).isEmpty()) {
      throw new IllegalStateException(
          "Execution " + previous.executionId()
              + " does not retain the failed logical Command effect identity");
    }
    if (intent == ExecutionRedriveIntent.REISSUE_COMMAND && previous.status() != ExecutionStatus.SUCCEEDED) {
      throw new IllegalStateException(
          "Command reissue requires a SUCCEEDED execution");
    }
    if (intent == ExecutionRedriveIntent.REISSUE_COMMAND && allowFailed) {
      throw new IllegalArgumentException("Command reissue does not accept allowFailed=true");
    }
    if (intent == ExecutionRedriveIntent.REISSUE_COMMAND && targetCommandId.isEmpty()) {
      throw new IllegalArgumentException("Command reissue requires targetCommandId");
    }
    if (intent == ExecutionRedriveIntent.REISSUE_COMMAND && normalizedReason.isBlank()) {
      throw new IllegalArgumentException("Command reissue requires a nonblank reason");
    }
    if (intent != ExecutionRedriveIntent.REISSUE_COMMAND && targetCommandId.isPresent()) {
      throw new IllegalArgumentException("targetCommandId is only valid for REISSUE_COMMAND");
    }
  }

  static ExecutionRedrivePlan from(
      ExecutionRecord<Object, Object> previous,
      OptionalLong expectedVersion,
      boolean allowFailed,
      String reason) {
    return from(previous, expectedVersion, allowFailed, ExecutionRedriveIntent.REPLAY, null, reason);
  }

  static ExecutionRedrivePlan from(
      ExecutionRecord<Object, Object> previous,
      OptionalLong expectedVersion,
      boolean allowFailed,
      ExecutionRedriveIntent intent,
      String reason) {
    return from(previous, expectedVersion, allowFailed, intent, null, reason);
  }

  static ExecutionRedrivePlan from(
      ExecutionRecord<Object, Object> previous,
      OptionalLong expectedVersion,
      boolean allowFailed,
      ExecutionRedriveIntent intent,
      String targetCommandId,
      String reason) {
    Objects.requireNonNull(previous, "previous must not be null");
    Objects.requireNonNull(expectedVersion, "expectedVersion must not be null");
    ExecutionRedriveIntent resolvedIntent = Objects.requireNonNull(intent, "intent must not be null");
    if (resolvedIntent == ExecutionRedriveIntent.REISSUE_COMMAND && expectedVersion.isEmpty()) {
      throw new IllegalArgumentException("Command reissue requires expectedVersion");
    }
    long version = expectedVersion.orElse(previous.version());
    String normalizedReason = normalizeReason(reason, resolvedIntent == ExecutionRedriveIntent.REISSUE_COMMAND);
    Optional<String> normalizedTarget = normalizeTarget(targetCommandId);
    String transitionKey = switch (resolvedIntent) {
      case REPLAY -> "redrive:" + previous.executionId() + ":" + version;
      case RETRY_FAILED_COMMAND -> "command-retry:" + previous.executionId() + ":" + version;
      case REISSUE_COMMAND -> "command-reissue:" + previous.executionId() + ":" + version;
    };
    return new ExecutionRedrivePlan(
        previous,
        version,
        allowFailed,
        resolvedIntent,
        normalizedTarget,
        normalizedReason,
        transitionKey);
  }

  private static boolean redrivable(
      ExecutionStatus status,
      boolean allowFailed,
      ExecutionRedriveIntent intent) {
    return (intent == ExecutionRedriveIntent.REISSUE_COMMAND && status == ExecutionStatus.SUCCEEDED)
        || status == ExecutionStatus.DLQ
        || (allowFailed && (status == ExecutionStatus.FAILED
            || status == ExecutionStatus.REMOTE_OUTCOME_UNKNOWN));
  }

  private static String normalizeReason(String reason, boolean required) {
    if (reason == null || reason.isBlank()) {
      return required ? "" : "operator";
    }
    String trimmed = reason.trim();
    return trimmed.length() <= 80 ? trimmed : trimmed.substring(0, 80);
  }

  private static Optional<String> normalizeTarget(String targetCommandId) {
    if (targetCommandId == null || targetCommandId.isBlank()) {
      return Optional.empty();
    }
    return Optional.of(targetCommandId.trim());
  }
}
