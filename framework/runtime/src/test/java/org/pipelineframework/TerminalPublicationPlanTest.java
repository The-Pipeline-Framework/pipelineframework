package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;

class TerminalPublicationPlanTest {

    @Test
    void nonEmptyUnpublishedOutputPlansCheckpointAndTerminalPublish() {
        TransitionResultEnvelope result = TransitionResultEnvelope.completedInProcess(List.of("out-1"));

        TerminalPublicationPlan plan = TerminalPublicationPlan.from(execution(), result);

        assertTrue(plan.publishCheckpoint());
        assertTrue(plan.publishTerminalOutput());
        assertEquals(List.of("out-1"), plan.outputItems());
        assertEquals("out-1", plan.checkpointPayload());
        assertThrows(UnsupportedOperationException.class, () -> plan.outputItems().clear());
    }

    @Test
    void emptyOutputSkipsCheckpointButStillLetsTerminalPublisherNoOp() {
        TerminalPublicationPlan plan = TerminalPublicationPlan.from(
            execution(),
            TransitionResultEnvelope.completedInProcess(List.of()));

        assertFalse(plan.publishCheckpoint());
        assertTrue(plan.publishTerminalOutput());
        assertEquals(List.of(), plan.outputItems());
    }

    @Test
    void alreadyPublishedOutputSkipsTerminalPublish() {
        TerminalPublicationPlan plan = TerminalPublicationPlan.from(
            execution(),
            TransitionResultEnvelope.completedInProcess(List.of("out-1"), true));

        assertTrue(plan.publishCheckpoint());
        assertFalse(plan.publishTerminalOutput());
    }

    private static ExecutionRecord<Object, Object> execution() {
        return new ExecutionRecord<>(
            "tenant-1",
            "exec-1",
            "exec-key",
            "pipeline",
            "contract",
            "release",
            ExecutionResultShape.MATERIALIZED_MULTI,
            ExecutionStatus.RUNNING,
            1L,
            3,
            0,
            "worker",
            0L,
            0L,
            "previous",
            "input",
            null,
            null,
            null,
            null,
            10_000L,
            10_000L,
            86_400L);
    }
}
