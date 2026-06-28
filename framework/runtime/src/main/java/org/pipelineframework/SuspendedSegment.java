package org.pipelineframework;

import java.util.Objects;

import org.pipelineframework.orchestrator.TransitionAwaitSuspension;

record SuspendedSegment(
    ClaimedSegment segment,
    TransitionAwaitSuspension suspension) implements SegmentCommitPlan {

  SuspendedSegment {
    Objects.requireNonNull(segment, "segment must not be null");
    Objects.requireNonNull(suspension, "suspension must not be null");
  }
}
