package org.pipelineframework.orchestrator.controlplane;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;

class SegmentBoundaryFactFactoryTest {

    private final SegmentBoundaryFactFactory facts = new SegmentBoundaryFactFactory();

    @Test
    void segmentSuspendedBuildsBoundaryAndDispatchFactsTogether() {
        AwaitUnitRecord unit = unit(true, 1);
        AwaitInteractionRecord interaction = interaction(0);
        List<ControlPlaneFact> built = facts.segmentSuspended(
            record("run-1"),
            "run-1:2:0",
            new TransitionAwaitSuspension("tenant", "run-1", "unit-1", 2, unit, List.of(interaction)));

        assertEquals(3, built.size());
        ControlPlaneFact.SegmentSuspended suspended =
            assertInstanceOf(ControlPlaneFact.SegmentSuspended.class, built.get(0));
        assertInstanceOf(ControlPlaneFact.BoundaryInteractionDispatched.class, built.get(1));
        assertInstanceOf(ControlPlaneFact.BoundaryDispatchCompleted.class, built.get(2));
        assertEquals(2, suspended.boundaryStepIndex());
        assertEquals("unit-1", suspended.boundaryUnitId());
        assertEquals(1, suspended.expectedItemCount());
    }

    @Test
    void segmentSuspendedDoesNotEmitDispatchCompleteWhenUnitDispatchIsOpen() {
        List<ControlPlaneFact> built = facts.segmentSuspended(
            record("run-1"),
            "run-1:2:0",
            new TransitionAwaitSuspension(
                "tenant",
                "run-1",
                "unit-1",
                2,
                unit(false, 1),
                List.of(interaction(0))));

        assertEquals(2, built.size());
        assertInstanceOf(ControlPlaneFact.SegmentSuspended.class, built.get(0));
        assertInstanceOf(ControlPlaneFact.BoundaryInteractionDispatched.class, built.get(1));
    }

    @Test
    void segmentSuspendedDoesNotEmitDispatchCompleteWhenExpectedCountIsUnknown() {
        List<ControlPlaneFact> built = facts.segmentSuspended(
            record("run-1"),
            "run-1:2:0",
            new TransitionAwaitSuspension(
                "tenant",
                "run-1",
                "unit-1",
                2,
                unit(true, null),
                List.of(interaction(0))));

        assertEquals(2, built.size());
        assertInstanceOf(ControlPlaneFact.SegmentSuspended.class, built.get(0));
        assertInstanceOf(ControlPlaneFact.BoundaryInteractionDispatched.class, built.get(1));
    }

    @Test
    void boundaryCompletionUsesItemIndexAsIdempotencyKeyForItemizedAwait() {
        ControlPlaneFact fact = facts.boundaryCompletionAdmitted(interaction(3), unit(false, null)).getFirst();

        ControlPlaneFact.BoundaryCompletionAdmitted admitted =
            assertInstanceOf(ControlPlaneFact.BoundaryCompletionAdmitted.class, fact);
        assertEquals("item:3", admitted.idempotencyKey());
    }

    @Test
    void terminalPublicationFactsUseStablePublicationIdentifiers() {
        SegmentTerminalPublicationFacts publicationFacts = facts.terminalPublicationFacts(
            record("run-terminal"),
            "run-terminal:0:0",
            "object-publish");

        assertEquals("run-terminal:terminal-output:object-publish", publicationFacts.publicationId());
        assertEquals("run-terminal:terminal-output:object-publish:terminal-publication",
            publicationFacts.idempotencyKey());
        assertEquals(publicationFacts.prepared().publicationId(), publicationFacts.completed().publicationId());
    }

    private static ExecutionRecord<Object, Object> record(String executionId) {
        return new ExecutionRecord<>(
            "tenant",
            executionId,
            "key-" + executionId,
            "pipeline",
            "contract",
            "release",
            ExecutionResultShape.SINGLE,
            ExecutionStatus.QUEUED,
            0L,
            2,
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
            999999L);
    }

    private static AwaitUnitRecord unit(boolean dispatchComplete, Integer expectedItemCount) {
        return new AwaitUnitRecord(
            "tenant",
            "unit-1",
            "run-1",
            "AwaitPaymentProvider",
            2,
            "ONE_TO_ONE",
            1L,
            AwaitUnitStatus.WAITING_EXTERNAL,
            null,
            expectedItemCount,
            0,
            java.util.Set.of(),
            dispatchComplete,
            1L,
            1L,
            999999L);
    }

    private static AwaitInteractionRecord interaction(int itemIndex) {
        return new AwaitInteractionRecord(
            "tenant",
            "run-1",
            "AwaitPaymentProvider",
            2,
            String.class.getName(),
            "interaction-" + itemIndex,
            "corr-" + itemIndex,
            "cause-" + itemIndex,
            "idem-" + itemIndex,
            1L,
            AwaitInteractionStatus.WAITING,
            "request-" + itemIndex,
            "response-" + itemIndex,
            "unit-1",
            itemIndex,
            "user-1",
            null,
            null,
            "kafka",
            Map.of(),
            999999L,
            1L,
            1L,
            999999L);
    }
}
