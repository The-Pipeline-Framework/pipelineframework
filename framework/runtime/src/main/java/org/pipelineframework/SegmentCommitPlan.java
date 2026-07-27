package org.pipelineframework;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerOutcome;

/**
 * Pure immutable decision for committing a worker transition result.
 */
sealed interface SegmentCommitPlan permits CompletedSegment, SuspendedSegment, FailedSegment {

  ClaimedSegment segment();

  static SegmentCommitPlan from(ClaimedSegment segment, TransitionResultEnvelope result) {
    return from(segment, result, Optional.empty());
  }

  static SegmentCommitPlan from(
      ClaimedSegment segment,
      TransitionResultEnvelope result,
      TransitionPayloadCodec payloadCodec) {
    return from(segment, result, Optional.of(payloadCodec));
  }

  private static SegmentCommitPlan from(
      ClaimedSegment segment,
      TransitionResultEnvelope result,
      Optional<TransitionPayloadCodec> payloadCodec) {
    Objects.requireNonNull(segment, "segment must not be null");
    if (result == null) {
      return new FailedSegment(
          segment,
          new IllegalStateException("PipelineTransitionWorker returned null result"));
    }
    if (result.outcome() == TransitionWorkerOutcome.COMPLETED) {
      return completed(segment, result, payloadCodec);
    }
    if (result.outcome() == TransitionWorkerOutcome.WAITING_EXTERNAL) {
      return new SuspendedSegment(segment, result.awaitSuspension());
    }
    return new FailedSegment(segment, result.failure().toException());
  }

  static CompletedSegment completed(
      ClaimedSegment segment,
      TransitionResultEnvelope result,
      Optional<TransitionPayloadCodec> payloadCodec) {
    Objects.requireNonNull(segment, "segment must not be null");
    Objects.requireNonNull(result, "result must not be null");
    if (result.terminalInputPassthrough() && payloadCodec.isEmpty()) {
      throw new IllegalArgumentException("Terminal input pass-through requires a payload codec");
    }
    List<?> outputItems = result.terminalInputPassthrough()
        ? terminalInputItems(segment, payloadCodec.orElseThrow())
        : result.coordinatorOutputItems();
    if (segment.record().resultShape() == ExecutionResultShape.SINGLE && outputItems.size() > 1) {
      throw new IllegalStateException(
          "Async queue execution " + segment.record().executionId()
              + " produced " + outputItems.size()
              + " terminal items for SINGLE result shape");
    }
    return new CompletedSegment(
        segment,
        result,
        outputItems,
        TerminalPublicationPlan.from(segment, result, outputItems));
  }

  private static List<?> terminalInputItems(ClaimedSegment segment, TransitionPayloadCodec payloadCodec) {
    Object inputPayload = segment.record().inputPayload();
    if (inputPayload instanceof ExecutionInputSnapshot snapshot) {
      if (snapshot.shape() == ExecutionInputShape.MULTI && snapshot.payload() instanceof Iterable<?> items) {
        return decodeItems(items, payloadCodec);
      }
      return List.of(decodeItem(snapshot.payload(), payloadCodec));
    }
    if (inputPayload instanceof Iterable<?> items) {
      return decodeItems(items, payloadCodec);
    }
    return List.of(decodeItem(inputPayload, payloadCodec));
  }

  private static List<?> decodeItems(Iterable<?> items, TransitionPayloadCodec payloadCodec) {
    return java.util.stream.StreamSupport.stream(items.spliterator(), false)
        .map(item -> decodeItem(item, payloadCodec))
        .toList();
  }

  private static Object decodeItem(Object item, TransitionPayloadCodec payloadCodec) {
    return item instanceof SerializedTransitionPayload serialized
        ? payloadCodec.decode(serialized)
        : item;
  }
}
