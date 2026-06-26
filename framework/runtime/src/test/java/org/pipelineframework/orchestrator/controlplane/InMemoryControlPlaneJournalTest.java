package org.pipelineframework.orchestrator.controlplane;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.ExecutionResultShape;

class InMemoryControlPlaneJournalTest {

    @Test
    void appendsFactsWithMonotonicSequenceAndRebuildsProjection() {
        InMemoryControlPlaneJournal journal = new InMemoryControlPlaneJournal();

        ControlPlaneAppendResult created = journal.append("tenant", "run-1", 0, List.of(submitted()), 100L)
            .await().indefinitely();
        ControlPlaneAppendResult started = journal.append(
                "tenant",
                "run-1",
                created.projection().version(),
                List.of(new ControlPlaneFact.SegmentAttemptStarted("tenant", "run-1", "segment-0", "attempt-0", 0)),
                101L)
            .await().indefinitely();

        assertEquals(1, created.appendedEvents().getFirst().sequence());
        assertEquals(2, started.appendedEvents().getFirst().sequence());
        assertEquals(PipelineRunStatus.RUNNING, started.projection().run().orElseThrow().status());
        assertEquals(SegmentStatus.RUNNING, started.projection().segments().get("segment-0").status());
    }

    @Test
    void duplicateAppendIsIdempotentEvenWhenExpectedVersionIsStale() {
        InMemoryControlPlaneJournal journal = new InMemoryControlPlaneJournal();
        ControlPlaneFact.RunSubmitted fact = submitted();

        ControlPlaneAppendResult first = journal.append("tenant", "run-1", 0, List.of(fact), 100L)
            .await().indefinitely();
        ControlPlaneAppendResult duplicate = journal.append("tenant", "run-1", 0, List.of(fact), 101L)
            .await().indefinitely();

        assertEquals(1, first.appendedEvents().size());
        assertTrue(duplicate.appendedEvents().isEmpty());
        assertEquals(1, duplicate.projection().version());
    }

    @Test
    void staleAppendWithNewFactsFailsOptimisticAppend() {
        InMemoryControlPlaneJournal journal = new InMemoryControlPlaneJournal();

        journal.append("tenant", "run-1", 0, List.of(submitted()), 100L).await().indefinitely();

        assertThrows(ControlPlaneAppendConflictException.class, () -> journal.append(
                "tenant",
                "run-1",
                0,
                List.of(new ControlPlaneFact.SegmentAttemptStarted("tenant", "run-1", "segment-0", "attempt-0", 0)),
                101L)
            .await().indefinitely());
    }

    @Test
    void findsDueSegmentsAndTimedOutBoundaryInteractionsFromCurrentProjections() {
        InMemoryControlPlaneJournal journal = new InMemoryControlPlaneJournal();

        ControlPlaneAppendResult created = journal.append("tenant", "run-1", 0, List.of(submitted()), 100L)
            .await().indefinitely();
        ControlPlaneAppendResult suspended = journal.append(
                "tenant",
                "run-1",
                created.projection().version(),
                List.of(
                    new ControlPlaneFact.SegmentAttemptStarted("tenant", "run-1", "segment-0", "attempt-0", 0),
                    new ControlPlaneFact.SegmentSuspended(
                        "tenant",
                        "run-1",
                        "segment-0",
                        "attempt-0",
                        "await-unit",
                        BoundaryKind.AWAIT,
                        1,
                        1),
                    new ControlPlaneFact.BoundaryInteractionDispatched(
                        "tenant",
                        "run-1",
                        "await-unit",
                        BoundaryKind.AWAIT,
                        "interaction-0",
                        "correlation-0",
                        "idem-0",
                        0,
                        "request-0",
                        "kafka",
                        150L)),
                110L)
            .await().indefinitely();

        assertTrue(journal.findDueSegments(100L, 10).await().indefinitely().isEmpty());
        List<DueBoundaryInteraction> timedOut = journal.findTimedOutInteractions(151L, 10).await().indefinitely();

        assertEquals(3, suspended.appendedEvents().size());
        assertEquals(1, timedOut.size());
        assertEquals("interaction-0", timedOut.getFirst().interactionId());
    }

    @Test
    void completionFactsAreIdempotentByBoundaryIdempotencyKey() {
        InMemoryControlPlaneJournal journal = new InMemoryControlPlaneJournal();
        ControlPlaneAppendResult created = journal.append("tenant", "run-1", 0, List.of(submitted()), 100L)
            .await().indefinitely();
        ControlPlaneAppendResult opened = journal.append(
                "tenant",
                "run-1",
                created.projection().version(),
                List.of(
                    new ControlPlaneFact.SegmentAttemptStarted("tenant", "run-1", "segment-0", "attempt-0", 0),
                    new ControlPlaneFact.SegmentSuspended(
                        "tenant",
                        "run-1",
                        "segment-0",
                        "attempt-0",
                        "await-unit",
                        BoundaryKind.AWAIT,
                        1,
                        1),
                    dispatched(),
                    new ControlPlaneFact.BoundaryDispatchCompleted("tenant", "run-1", "await-unit", 1)),
                101L)
            .await().indefinitely();
        ControlPlaneFact.BoundaryCompletionAdmitted admitted = BoundaryAdmissionFacts.completion(
            new BoundaryAdmissionRequest("tenant", "run-1", "await-unit", BoundaryKind.AWAIT, "interaction-0", "idem-0", "ok"));

        ControlPlaneAppendResult first = journal.append(
            "tenant",
            "run-1",
            opened.projection().version(),
            List.of(admitted),
            102L).await().indefinitely();
        ControlPlaneAppendResult duplicate = journal.append(
            "tenant",
            "run-1",
            opened.projection().version(),
            List.of(admitted),
            103L).await().indefinitely();

        assertEquals(BoundaryUnitStatus.COMPLETED, first.projection().boundaries().get("await-unit").status());
        assertTrue(duplicate.appendedEvents().isEmpty());
        assertEquals(first.projection().version(), duplicate.projection().version());
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

    private static ControlPlaneFact.BoundaryInteractionDispatched dispatched() {
        return new ControlPlaneFact.BoundaryInteractionDispatched(
            "tenant",
            "run-1",
            "await-unit",
            BoundaryKind.AWAIT,
            "interaction-0",
            "correlation-0",
            "idem-0",
            0,
            "request-0",
            "kafka",
            150L);
    }
}
