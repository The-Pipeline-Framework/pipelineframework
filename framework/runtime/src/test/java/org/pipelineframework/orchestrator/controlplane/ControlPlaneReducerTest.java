package org.pipelineframework.orchestrator.controlplane;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.ExecutionResultShape;

class ControlPlaneReducerTest {

    @Test
    void terminalSegmentAndPublicationProduceSucceededRunProjection() {
        ControlPlaneProjection projection = ControlPlaneReducer.reduce("tenant", "run-1", List.of(
            event(1, submitted()),
            event(2, new ControlPlaneFact.SegmentAttemptStarted("tenant", "run-1", "segment-0", "attempt-0", 0)),
            event(3, new ControlPlaneFact.SegmentCompleted(
                "tenant",
                "run-1",
                "segment-0",
                "attempt-0",
                List.of("out-1", "out-2"),
                true)),
            event(4, new ControlPlaneFact.TerminalPublicationCompleted(
                "tenant",
                "run-1",
                "segment-0",
                "object-publish",
                "run-1:publish")),
            event(5, new ControlPlaneFact.RunSucceeded("tenant", "run-1", "segment-0", List.of("out-1", "out-2")))));

        assertEquals(5, projection.version());
        assertEquals(PipelineRunStatus.SUCCEEDED, projection.run().orElseThrow().status());
        assertEquals(SegmentStatus.COMPLETED, projection.segments().get("segment-0").status());
        assertEquals(SegmentAttemptStatus.COMPLETED, projection.attempts().get("attempt-0").status());
        assertTrue(projection.terminalPublicationKeys().contains("terminal-publication-completed:object-publish:run-1:publish"));
    }

    @Test
    void awaitAndCheckpointCompletionsUseTheSameBoundaryCompletionFact() {
        ControlPlaneProjection projection = ControlPlaneReducer.reduce("tenant", "run-1", List.of(
            event(1, submitted()),
            event(2, new ControlPlaneFact.SegmentAttemptStarted("tenant", "run-1", "segment-0", "attempt-0", 0)),
            event(3, new ControlPlaneFact.SegmentSuspended(
                "tenant",
                "run-1",
                "segment-0",
                "attempt-0",
                "await-unit",
                BoundaryKind.AWAIT,
                1,
                2)),
            event(4, dispatched("await-unit", BoundaryKind.AWAIT, "interaction-0", "idem-0", 0)),
            event(5, dispatched("await-unit", BoundaryKind.AWAIT, "interaction-1", "idem-1", 1)),
            event(6, BoundaryAdmissionFacts.completion(new BoundaryAdmissionRequest(
                "tenant",
                "run-1",
                "await-unit",
                BoundaryKind.AWAIT,
                "interaction-0",
                "idem-0",
                "status-0"))),
            event(7, new ControlPlaneFact.BoundaryDispatchCompleted("tenant", "run-1", "await-unit", 2)),
            event(8, BoundaryAdmissionFacts.completion(new BoundaryAdmissionRequest(
                "tenant",
                "run-1",
                "await-unit",
                BoundaryKind.AWAIT,
                "interaction-1",
                "idem-1",
                "status-1")))));

        assertEquals(PipelineRunStatus.WAITING_BOUNDARY, projection.run().orElseThrow().status());
        assertEquals(BoundaryUnitStatus.COMPLETED, projection.boundaries().get("await-unit").status());
        assertEquals(2, projection.boundaries().get("await-unit").completedItemCount());
        assertEquals(BoundaryInteractionStatus.COMPLETED, projection.interactions().get("interaction-0").status());
        assertEquals(BoundaryInteractionStatus.COMPLETED, projection.interactions().get("interaction-1").status());
    }

    @Test
    void timeoutFactDerivesFailedRunAndTimedOutBoundaryProjection() {
        ControlPlaneProjection projection = ControlPlaneReducer.reduce("tenant", "run-1", List.of(
            event(1, submitted()),
            event(2, new ControlPlaneFact.SegmentAttemptStarted("tenant", "run-1", "segment-0", "attempt-0", 0)),
            event(3, new ControlPlaneFact.SegmentSuspended(
                "tenant",
                "run-1",
                "segment-0",
                "attempt-0",
                "await-unit",
                BoundaryKind.AWAIT,
                1,
                1)),
            event(4, dispatched("await-unit", BoundaryKind.AWAIT, "interaction-0", "idem-0", 0)),
            event(5, new ControlPlaneFact.InteractionTimedOut(
                "tenant",
                "run-1",
                "await-unit",
                "interaction-0",
                "provider timeout"))));

        assertEquals(PipelineRunStatus.FAILED, projection.run().orElseThrow().status());
        assertEquals("BOUNDARY_TIMEOUT", projection.run().orElseThrow().errorCode());
        assertEquals(SegmentStatus.FAILED, projection.segments().get("segment-0").status());
        assertEquals(BoundaryUnitStatus.TIMED_OUT, projection.boundaries().get("await-unit").status());
        assertEquals(BoundaryInteractionStatus.TIMED_OUT, projection.interactions().get("interaction-0").status());
    }

    @Test
    void continuationSegmentCreatesNewDuePipelineSegmentAfterBoundaryCompletion() {
        ControlPlaneProjection projection = ControlPlaneReducer.reduce("tenant", "run-1", List.of(
            event(1, submitted()),
            event(2, new ControlPlaneFact.SegmentAttemptStarted("tenant", "run-1", "segment-0", "attempt-0", 0)),
            event(3, new ControlPlaneFact.SegmentSuspended(
                "tenant",
                "run-1",
                "segment-0",
                "attempt-0",
                "await-unit",
                BoundaryKind.AWAIT,
                1,
                1)),
            event(4, new ControlPlaneFact.ContinuationSegmentCreated(
                "tenant",
                "run-1",
                "segment-0",
                "segment-1",
                "await-unit",
                2,
                -1,
                "status-0"))));

        assertEquals(PipelineRunStatus.RUNNING, projection.run().orElseThrow().status());
        assertEquals(SegmentStatus.QUEUED, projection.segments().get("segment-1").status());
        assertEquals("status-0", projection.segments().get("segment-1").inputPayload());
    }

    private static ControlPlaneFact.RunSubmitted submitted() {
        return new ControlPlaneFact.RunSubmitted(
            "tenant",
            "run-1",
            "execution-key",
            "pipeline",
            "contract",
            "release",
            ExecutionResultShape.MATERIALIZED_MULTI,
            "segment-0",
            0,
            -1,
            "input",
            1000L);
    }

    private static ControlPlaneFact.BoundaryInteractionDispatched dispatched(
        String unitId,
        BoundaryKind kind,
        String interactionId,
        String idempotencyKey,
        int itemIndex
    ) {
        return new ControlPlaneFact.BoundaryInteractionDispatched(
            "tenant",
            "run-1",
            unitId,
            kind,
            interactionId,
            "correlation-" + itemIndex,
            idempotencyKey,
            itemIndex,
            "request-" + itemIndex,
            "kafka",
            900L);
    }

    private static ControlPlaneEvent event(long sequence, ControlPlaneFact fact) {
        return new ControlPlaneEvent(sequence, 100L + sequence, fact);
    }
}
