package org.pipelineframework;

import java.util.List;

import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;
import org.pipelineframework.orchestrator.TransitionFailureEnvelope;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerOutcome;

sealed interface TransitionCommitPlan
    permits TransitionCommitPlan.MarkSucceeded,
    TransitionCommitPlan.MarkWaitingExternal,
    TransitionCommitPlan.HandleFailure {

    ExecutionRecord<Object, Object> record();

    String transitionKey();

    static TransitionCommitPlan from(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        TransitionResultEnvelope result,
        long nowEpochMs) {
        if (result == null) {
            return new HandleFailure(
                record,
                transitionKey,
                new IllegalStateException("PipelineTransitionWorker returned null result"));
        }
        if (result.outcome() == TransitionWorkerOutcome.WAITING_EXTERNAL) {
            TransitionAwaitSuspension suspension = result.awaitSuspension();
            if (suspension == null) {
                return new HandleFailure(
                    record,
                    transitionKey,
                    new IllegalStateException("WAITING_EXTERNAL result missing await suspension"));
            }
            return new MarkWaitingExternal(record, transitionKey, suspension, nowEpochMs);
        }
        if (result.outcome() == TransitionWorkerOutcome.FAILED) {
            TransitionFailureEnvelope failure = result.failure();
            if (failure == null) {
                return new HandleFailure(
                    record,
                    transitionKey,
                    new IllegalStateException("FAILED result missing failure payload"));
            }
            return new HandleFailure(record, transitionKey, failure.toException());
        }

        List<?> outputItems = result.coordinatorOutputItems();
        if (record.resultShape() == ExecutionResultShape.SINGLE && outputItems.size() > 1) {
            return new HandleFailure(
                record,
                transitionKey,
                new IllegalStateException(
                    "Async queue execution " + record.executionId()
                        + " produced " + outputItems.size()
                        + " terminal items for SINGLE result shape"));
        }
        return new MarkSucceeded(record, transitionKey, result, outputItems, nowEpochMs);
    }

    record MarkSucceeded(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        TransitionResultEnvelope result,
        List<?> outputItems,
        long nowEpochMs) implements TransitionCommitPlan {
        public MarkSucceeded {
            outputItems = List.copyOf(outputItems);
        }
    }

    record MarkWaitingExternal(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        TransitionAwaitSuspension suspension,
        long nowEpochMs) implements TransitionCommitPlan {
    }

    record HandleFailure(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        Throwable failure) implements TransitionCommitPlan {
    }
}
