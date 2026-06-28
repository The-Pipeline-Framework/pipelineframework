package org.pipelineframework;

import java.util.Objects;

import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.TransitionWorkerCommand;

/**
 * Immutable view of one queue-async segment claimed by a worker.
 */
record ClaimedSegment(
    ExecutionRecord<Object, Object> record,
    String transitionKey) {

  ClaimedSegment {
    Objects.requireNonNull(record, "record must not be null");
    if (transitionKey == null || transitionKey.isBlank()) {
      throw new IllegalArgumentException("transitionKey must not be blank");
    }
  }

  static ClaimedSegment from(ExecutionRecord<Object, Object> record) {
    Objects.requireNonNull(record, "record must not be null");
    return new ClaimedSegment(
        record,
        transitionKey(record.executionId(), record.currentStepIndex(), record.attempt()));
  }

  boolean resumesFromAwait() {
    return record.currentStepIndex() > 0
        && record.awaitUnitId() != null
        && !record.awaitUnitId().isBlank();
  }

  TransitionCommandEnvelope transitionCommand(Object payload, TransitionPayloadCodec payloadCodec) {
    Objects.requireNonNull(payloadCodec, "payloadCodec must not be null");
    TransitionWorkerCommand command = new TransitionWorkerCommand(
        record.tenantId(),
        record.executionId(),
        record.currentStepIndex(),
        record.attempt(),
        record.resultShape(),
        record.version(),
        transitionKey,
        payload);
    SerializedTransitionPayload encodedPayload = payloadCodec.encode(payload);
    return TransitionCommandEnvelope.from(
        command,
        record.pipelineId(),
        record.contractVersion(),
        record.releaseVersion(),
        transitionKey,
        encodedPayload);
  }

  private static String transitionKey(String executionId, int stepIndex, int attempt) {
    return executionId + ":" + stepIndex + ":" + attempt;
  }
}
