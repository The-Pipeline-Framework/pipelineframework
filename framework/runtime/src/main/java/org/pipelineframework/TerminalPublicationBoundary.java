package org.pipelineframework;

import java.util.Objects;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.checkpoint.CheckpointPublicationService;
import org.pipelineframework.objectpublish.ObjectPublishCompletionService;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;

/**
 * Interprets terminal publication plans before a segment can commit success.
 */
class TerminalPublicationBoundary {

  private final CheckpointPublicationService checkpointPublicationService;
  private final ObjectPublishCompletionService objectPublishCompletionService;
  private final Supplier<TransitionPayloadCodec> payloadCodec;
  private final Supplier<SegmentBoundaryLedger> segmentBoundaryLedger;

  TerminalPublicationBoundary(
      CheckpointPublicationService checkpointPublicationService,
      ObjectPublishCompletionService objectPublishCompletionService,
      Supplier<TransitionPayloadCodec> payloadCodec,
      Supplier<SegmentBoundaryLedger> segmentBoundaryLedger) {
    this.checkpointPublicationService = checkpointPublicationService;
    this.objectPublishCompletionService = objectPublishCompletionService;
    this.payloadCodec = Objects.requireNonNull(payloadCodec, "payloadCodec must not be null");
    this.segmentBoundaryLedger = Objects.requireNonNull(segmentBoundaryLedger, "segmentBoundaryLedger must not be null");
  }

  Uni<Void> publishBeforeSuccess(CompletedSegment completed, long nowEpochMs) {
    Objects.requireNonNull(completed, "completed must not be null");
    TerminalPublicationPlan plan = completed.terminalPublication();
    return publishCheckpoint(completed, plan)
        .chain(() -> publishObjectOutput(plan, nowEpochMs));
  }

  private Uni<Void> publishCheckpoint(CompletedSegment completed, TerminalPublicationPlan plan) {
    if (checkpointPublicationService == null) {
      return Uni.createFrom().voidItem();
    }
    return plan.checkpointPayload()
        .map(payload -> checkpointPublicationService.publishIfConfigured(completed.segment().record(), payload))
        .orElseGet(() -> Uni.createFrom().voidItem());
  }

  private Uni<Void> publishObjectOutput(TerminalPublicationPlan plan, long nowEpochMs) {
    ClaimedSegment segment = plan.segment();
    if (plan.alreadyPublished()) {
      return segmentBoundaryLedger.get().recordTerminalPublicationCompleted(
          segment.record(),
          segment.transitionKey(),
          nowEpochMs);
    }
    if (objectPublishCompletionService == null) {
      return Uni.createFrom().voidItem();
    }
    return objectPublishCompletionService.publishIfConfigured(() -> plan.decodedOutputItems(payloadCodec.get()))
        .chain(() -> segmentBoundaryLedger.get().recordTerminalPublicationCompleted(
            segment.record(),
            segment.transitionKey(),
            nowEpochMs));
  }
}
