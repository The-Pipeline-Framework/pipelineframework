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
    return publishCheckpoint(completed, plan, nowEpochMs)
        .chain(() -> publishObjectOutput(plan, nowEpochMs));
  }

  private Uni<Void> publishCheckpoint(CompletedSegment completed, TerminalPublicationPlan plan, long nowEpochMs) {
    var checkpointPayload = plan.checkpointPayload();
    if (checkpointPublicationService == null || !checkpointPublicationService.enabled() || checkpointPayload.isEmpty()) {
      return Uni.createFrom().voidItem();
    }
    return new TerminalPublicationIntent(
        TerminalPublicationIntent.CHECKPOINT,
        completed.segment(),
        () -> checkpointPublicationService.publishIfConfigured(completed.segment().record(), checkpointPayload.get()))
        .run(segmentBoundaryLedger.get(), nowEpochMs);
  }

  private Uni<Void> publishObjectOutput(TerminalPublicationPlan plan, long nowEpochMs) {
    ClaimedSegment segment = plan.segment();
    if (!plan.alreadyPublished()
        && (objectPublishCompletionService == null || !objectPublishCompletionService.enabled())) {
      return Uni.createFrom().voidItem();
    }
    return new TerminalPublicationIntent(
        TerminalPublicationIntent.OBJECT_PUBLISH,
        segment,
        () -> plan.alreadyPublished()
            ? Uni.createFrom().voidItem()
            : objectPublishCompletionService.publishIfConfigured(() -> plan.decodedOutputItems(payloadCodec.get())))
        .run(segmentBoundaryLedger.get(), nowEpochMs);
  }
}
