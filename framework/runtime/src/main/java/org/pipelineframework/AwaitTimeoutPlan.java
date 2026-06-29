package org.pipelineframework;

import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionStatus;

record AwaitTimeoutPlan(
    AwaitInteractionRecord interaction,
    Optional<ExecutionRecord<Object, Object>> parent,
    boolean failParent,
    Optional<String> transitionKey,
    String errorCode,
    String errorMessage) {

  private static final String ERROR_CODE = "AWAIT_TIMEOUT";

  AwaitTimeoutPlan {
    Objects.requireNonNull(interaction, "interaction must not be null");
    Objects.requireNonNull(parent, "parent must not be null");
    Objects.requireNonNull(transitionKey, "transitionKey must not be null");
    Objects.requireNonNull(errorCode, "errorCode must not be null");
    Objects.requireNonNull(errorMessage, "errorMessage must not be null");
    if (failParent && (parent.isEmpty() || transitionKey.isEmpty())) {
      throw new IllegalArgumentException("parent failure plans require a parent record and transition key");
    }
    if (failParent) {
      ExecutionRecord<Object, Object> execution = parent.orElseThrow();
      if (execution.status() != ExecutionStatus.WAITING_EXTERNAL
          || !Objects.equals(interaction.unitId(), execution.awaitUnitId())) {
        throw new IllegalArgumentException(
            "parent failure plans require a waiting parent bound to the timed-out interaction");
      }
    }
  }

  static AwaitTimeoutPlan from(
      AwaitInteractionRecord interaction,
      Optional<ExecutionRecord<Object, Object>> parent) {
    Objects.requireNonNull(interaction, "interaction must not be null");
    Objects.requireNonNull(parent, "parent must not be null");
    String message = "Await interaction timed out: " + interaction.interactionId();
    if (parent.isEmpty() || parent.get().status().terminal()) {
      return recordOnly(interaction, parent, message);
    }
    ExecutionRecord<Object, Object> execution = parent.get();
    if (execution.status() != ExecutionStatus.WAITING_EXTERNAL
        || !Objects.equals(interaction.unitId(), execution.awaitUnitId())) {
      return recordOnly(interaction, parent, message);
    }
    return new AwaitTimeoutPlan(
        interaction,
        parent,
        true,
        Optional.of(transitionKey(execution)),
        ERROR_CODE,
        message);
  }

  private static AwaitTimeoutPlan recordOnly(
      AwaitInteractionRecord interaction,
      Optional<ExecutionRecord<Object, Object>> parent,
      String message) {
    return new AwaitTimeoutPlan(
        interaction,
        parent,
        false,
        Optional.empty(),
        ERROR_CODE,
        message);
  }

  private static String transitionKey(ExecutionRecord<Object, Object> execution) {
    return execution.executionId() + ":" + execution.currentStepIndex() + ":" + execution.attempt();
  }
}
