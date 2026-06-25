package org.pipelineframework;

import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;

sealed interface QueueAsyncCommand
    permits QueueAsyncCommand.CompleteAwait,
    QueueAsyncCommand.TransitionSuspended,
    QueueAsyncCommand.ReleaseItemizedParent {

    record CompleteAwait(AwaitCompletionCommand command) implements QueueAsyncCommand {
    }

    record TransitionSuspended(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        TransitionAwaitSuspension suspension) implements QueueAsyncCommand {
    }

    record ReleaseItemizedParent(
        ExecutionRecord<Object, Object> parent,
        AwaitUnitRecord unit,
        int aggregateStepIndex,
        long nowEpochMs) implements QueueAsyncCommand {
    }
}
