package org.pipelineframework.awaitable;

/**
 * Durable lifecycle status for an await interaction.
 */
public enum AwaitInteractionStatus {
    WAITING,
    DISPATCHING,
    DISPATCHED,
    COMPLETED,
    FAILED,
    TIMED_OUT,
    CANCELLED,
    EXPIRED;

    /**
     * Whether this interaction can no longer be completed.
     *
     * @return true when terminal
     */
    public boolean terminal() {
        return this == COMPLETED || this == FAILED || this == TIMED_OUT || this == CANCELLED || this == EXPIRED;
    }
}
