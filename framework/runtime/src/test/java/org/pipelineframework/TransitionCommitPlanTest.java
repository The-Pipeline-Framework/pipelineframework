package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerOutcome;

class TransitionCommitPlanTest {

    @Test
    void completedTransitionPlansSuccessWithImmutableOutputItems() {
        ExecutionRecord<Object, Object> record = execution(ExecutionResultShape.MATERIALIZED_MULTI);
        TransitionResultEnvelope result = TransitionResultEnvelope.completedInProcess(List.of("out-1", "out-2"));

        TransitionCommitPlan plan = TransitionCommitPlan.from(record, "transition-1", result, 42_000L);

        TransitionCommitPlan.MarkSucceeded success =
            assertInstanceOf(TransitionCommitPlan.MarkSucceeded.class, plan);
        assertEquals(record, success.record());
        assertEquals("transition-1", success.transitionKey());
        assertEquals(result, success.result());
        assertEquals(List.of("out-1", "out-2"), success.outputItems());
        assertEquals(42_000L, success.nowEpochMs());
        assertThrows(UnsupportedOperationException.class, () -> success.outputItems().clear());
    }

    @Test
    void singleResultShapeViolationPlansFailure() {
        ExecutionRecord<Object, Object> record = execution(ExecutionResultShape.SINGLE);
        TransitionResultEnvelope result = TransitionResultEnvelope.completedInProcess(List.of("out-1", "out-2"));

        TransitionCommitPlan plan = TransitionCommitPlan.from(record, "transition-1", result, 42_000L);

        TransitionCommitPlan.HandleFailure failure =
            assertInstanceOf(TransitionCommitPlan.HandleFailure.class, plan);
        assertEquals(record, failure.record());
        assertEquals("transition-1", failure.transitionKey());
        assertTrue(failure.failure().getMessage().contains("produced 2 terminal items"));
    }

    @Test
    void waitingTransitionPlansExternalWaitCommit() {
        ExecutionRecord<Object, Object> record = execution(ExecutionResultShape.MATERIALIZED_MULTI);
        TransitionAwaitSuspension suspension = new TransitionAwaitSuspension("tenant-1", "exec-1", "unit-1", 3);

        TransitionCommitPlan plan = TransitionCommitPlan.from(
            record,
            "transition-1",
            TransitionResultEnvelope.waiting(suspension),
            42_000L);

        TransitionCommitPlan.MarkWaitingExternal waiting =
            assertInstanceOf(TransitionCommitPlan.MarkWaitingExternal.class, plan);
        assertEquals(record, waiting.record());
        assertEquals("transition-1", waiting.transitionKey());
        assertEquals(suspension, waiting.suspension());
        assertEquals(42_000L, waiting.nowEpochMs());
    }

    @Test
    void waitingTransitionMissingSuspensionPlansFailure() {
        ExecutionRecord<Object, Object> record = execution(ExecutionResultShape.MATERIALIZED_MULTI);
        TransitionResultEnvelope result = mock(TransitionResultEnvelope.class);
        when(result.outcome()).thenReturn(TransitionWorkerOutcome.WAITING_EXTERNAL);
        when(result.awaitSuspension()).thenReturn(null);

        TransitionCommitPlan plan = TransitionCommitPlan.from(record, "transition-1", result, 42_000L);

        TransitionCommitPlan.HandleFailure failure =
            assertInstanceOf(TransitionCommitPlan.HandleFailure.class, plan);
        assertTrue(failure.failure().getMessage().contains("missing await suspension"));
    }

    @Test
    void failedTransitionMissingFailurePayloadPlansFailure() {
        ExecutionRecord<Object, Object> record = execution(ExecutionResultShape.MATERIALIZED_MULTI);
        TransitionResultEnvelope result = mock(TransitionResultEnvelope.class);
        when(result.outcome()).thenReturn(TransitionWorkerOutcome.FAILED);
        when(result.failure()).thenReturn(null);

        TransitionCommitPlan plan = TransitionCommitPlan.from(record, "transition-1", result, 42_000L);

        TransitionCommitPlan.HandleFailure failure =
            assertInstanceOf(TransitionCommitPlan.HandleFailure.class, plan);
        assertTrue(failure.failure().getMessage().contains("missing failure payload"));
    }

    @Test
    void nullResultPlansFailure() {
        ExecutionRecord<Object, Object> record = execution(ExecutionResultShape.MATERIALIZED_MULTI);

        TransitionCommitPlan plan = TransitionCommitPlan.from(record, "transition-1", null, 42_000L);

        TransitionCommitPlan.HandleFailure failure =
            assertInstanceOf(TransitionCommitPlan.HandleFailure.class, plan);
        assertTrue(failure.failure().getMessage().contains("returned null result"));
    }

    private static ExecutionRecord<Object, Object> execution(ExecutionResultShape resultShape) {
        return new ExecutionRecord<>(
            "tenant-1",
            "exec-1",
            "exec-key",
            "pipeline",
            "contract",
            "release",
            resultShape,
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
