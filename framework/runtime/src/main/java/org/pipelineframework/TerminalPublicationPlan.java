package org.pipelineframework;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;

/**
 * Immutable plan for terminal publication side effects required before success.
 */
record TerminalPublicationPlan(
    ClaimedSegment segment,
    TransitionResultEnvelope result,
    List<?> persistedOutputItems,
    boolean alreadyPublished) {

  TerminalPublicationPlan {
    Objects.requireNonNull(segment, "segment must not be null");
    Objects.requireNonNull(result, "result must not be null");
    persistedOutputItems = persistedOutputItems == null ? List.of() : List.copyOf(persistedOutputItems);
  }

  static TerminalPublicationPlan from(
      ClaimedSegment segment,
      TransitionResultEnvelope result,
      List<?> persistedOutputItems) {
    return new TerminalPublicationPlan(segment, result, persistedOutputItems, result.terminalOutputPublished());
  }

  Optional<Object> checkpointPayload() {
    return persistedOutputItems.isEmpty() ? Optional.empty() : Optional.ofNullable(persistedOutputItems.getFirst());
  }

  boolean objectPublishRequired() {
    return !alreadyPublished;
  }

  List<?> decodedOutputItems(TransitionPayloadCodec payloadCodec) {
    Objects.requireNonNull(payloadCodec, "payloadCodec must not be null");
    return result.decodeOutputItems(payloadCodec);
  }
}
