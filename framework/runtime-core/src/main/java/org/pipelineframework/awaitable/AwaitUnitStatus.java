package org.pipelineframework.awaitable;

/**
 * Durable lifecycle status for an await unit.
 */
public enum AwaitUnitStatus {
    WAITING_EXTERNAL,
    COMPLETED,
    FAILED,
    TIMED_OUT,
    CANCELLED,
    EXPIRED;

    public boolean terminal() {
        return this == COMPLETED || this == FAILED || this == TIMED_OUT || this == CANCELLED || this == EXPIRED;
    }
}
