package org.pipelineframework;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TerminalPublicationPlanTest {

  @Test
  void terminalPublicationPlanSkipsObjectPublishWhenEnvelopeAlreadyPublished() {
    ClaimedSegment segment = ClaimedSegment.from(record());
    TransitionResultEnvelope result = TransitionResultEnvelope.completedInProcess(List.of("out"), true);

    TerminalPublicationPlan plan = TerminalPublicationPlan.from(segment, result, result.coordinatorOutputItems());

    assertFalse(plan.objectPublishRequired());
    assertTrue(plan.alreadyPublished());
  }

  @Test
  void terminalPublicationPlanExposesCheckpointPayload() {
    ClaimedSegment segment = ClaimedSegment.from(record());
    TransitionResultEnvelope result = TransitionResultEnvelope.completedInProcess(List.of("first", "second"));

    TerminalPublicationPlan plan = TerminalPublicationPlan.from(segment, result, result.coordinatorOutputItems());

    assertEquals("first", plan.checkpointPayload().orElseThrow());
  }

  @Test
  void terminalPublicationPlanDecodesPortableOutputLazily() {
    ClaimedSegment segment = ClaimedSegment.from(record());
    JsonTransitionPayloadCodec codec = new JsonTransitionPayloadCodec();
    TransitionResultEnvelope result = TransitionResultEnvelope.completed(codec, List.of("remote-output"));

    TerminalPublicationPlan plan = TerminalPublicationPlan.from(segment, result, result.coordinatorOutputItems());

    assertEquals(List.of("remote-output"), plan.decodedOutputItems(codec));
  }

  private static ExecutionRecord<Object, Object> record() {
    return new ExecutionRecord<>(
        "tenant-1",
        "exec-1",
        "key-1",
        "pipeline-a",
        "contract-a",
        "release-a",
        ExecutionResultShape.MATERIALIZED_MULTI,
        ExecutionStatus.QUEUED,
        0L,
        0,
        0,
        null,
        0L,
        0L,
        null,
        "input",
        null,
        null,
        null,
        null,
        1L,
        1L,
        99999999L);
  }
}
