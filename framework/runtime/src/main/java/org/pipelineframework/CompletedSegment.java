package org.pipelineframework;

import java.util.List;
import java.util.Objects;

import org.pipelineframework.orchestrator.TransitionResultEnvelope;

record CompletedSegment(
    ClaimedSegment segment,
    TransitionResultEnvelope result,
    List<?> outputItems,
    TerminalPublicationPlan terminalPublication) implements SegmentCommitPlan {

  CompletedSegment {
    Objects.requireNonNull(segment, "segment must not be null");
    Objects.requireNonNull(result, "result must not be null");
    outputItems = outputItems == null ? List.of() : List.copyOf(outputItems);
    Objects.requireNonNull(terminalPublication, "terminalPublication must not be null");
  }
}
