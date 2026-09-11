package org.pipelineframework.awaitable;

/** Identifies which side of the durable join owns delivery of the final value. */
public record CommandCompletionSettlementResult(AwaitInteractionRecord record, boolean completedByDispatch) {
    public CommandCompletionSettlementResult {
        java.util.Objects.requireNonNull(record, "record");
        if (completedByDispatch && record.status() != AwaitInteractionStatus.COMPLETED) {
            throw new IllegalArgumentException("dispatch delivery requires completed interaction");
        }
    }
}
