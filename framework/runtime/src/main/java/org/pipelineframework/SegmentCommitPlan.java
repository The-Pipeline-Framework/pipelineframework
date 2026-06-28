package org.pipelineframework;

import java.util.List;
import java.util.Objects;

import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerOutcome;

/**
 * Pure immutable decision for committing a worker transition result.
 */
sealed interface SegmentCommitPlan permits CompletedSegment, SuspendedSegment, FailedSegment {

  ClaimedSegment segment();

  static SegmentCommitPlan from(ClaimedSegment segment, TransitionResultEnvelope result) {
    Objects.requireNonNull(segment, "segment must not be null");
    if (result == null) {
      return new FailedSegment(
          segment,
          new IllegalStateException("PipelineTransitionWorker returned null result"));
    }
    if (result.outcome() == TransitionWorkerOutcome.COMPLETED) {
      return completed(segment, result);
    }
    if (result.outcome() == TransitionWorkerOutcome.WAITING_EXTERNAL) {
      return new SuspendedSegment(segment, result.awaitSuspension());
    }
    return new FailedSegment(segment, result.failure().toException());
  }

  static CompletedSegment completed(ClaimedSegment segment, TransitionResultEnvelope result) {
    Objects.requireNonNull(segment, "segment must not be null");
    Objects.requireNonNull(result, "result must not be null");
    List<?> outputItems = result.coordinatorOutputItems();
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
}
