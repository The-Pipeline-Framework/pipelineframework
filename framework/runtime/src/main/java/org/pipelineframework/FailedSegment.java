package org.pipelineframework;

import java.util.Objects;

record FailedSegment(
    ClaimedSegment segment,
    Throwable failure) implements SegmentCommitPlan {

  FailedSegment {
    Objects.requireNonNull(segment, "segment must not be null");
    Objects.requireNonNull(failure, "failure must not be null");
  }
}
