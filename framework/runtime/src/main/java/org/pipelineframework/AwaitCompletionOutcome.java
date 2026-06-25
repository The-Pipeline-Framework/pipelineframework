package org.pipelineframework;

import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitUnitRecord;

sealed interface AwaitCompletionOutcome
    permits AwaitCompletionOutcome.RecordOnly,
    AwaitCompletionOutcome.SignalLiveSession,
    AwaitCompletionOutcome.DispatchItemContinuation,
    AwaitCompletionOutcome.ReleaseParent {

    record RecordOnly(String reason) implements AwaitCompletionOutcome {
    }

    record SignalLiveSession(AwaitInteractionRecord record, AwaitUnitRecord unit) implements AwaitCompletionOutcome {
    }

    record DispatchItemContinuation(
        AwaitInteractionRecord record,
        AwaitUnitRecord unit,
        long nowEpochMs) implements AwaitCompletionOutcome {
    }

    record ReleaseParent(AwaitInteractionRecord record, AwaitUnitRecord unit, long nowEpochMs)
        implements AwaitCompletionOutcome {
    }
}
