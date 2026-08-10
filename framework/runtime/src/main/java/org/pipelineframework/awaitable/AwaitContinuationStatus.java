package org.pipelineframework.awaitable;

/**
 * Durable processing lifecycle for an item await continuation.
 *
 * <p>This is deliberately independent from {@link AwaitInteractionStatus}: provider completion is
 * an external fact, while continuation processing is local durable work.</p>
 */
public enum AwaitContinuationStatus {
    HELD,
    READY,
    CLAIMED,
    RETRY_DUE,
    APPLIED,
    FAILED,
    CANCELLED;

    public boolean due(long nowEpochMs) {
        return (this == READY || this == RETRY_DUE) && nowEpochMs >= 0;
    }

    public boolean terminal() {
        return this == APPLIED || this == FAILED || this == CANCELLED;
    }
}
