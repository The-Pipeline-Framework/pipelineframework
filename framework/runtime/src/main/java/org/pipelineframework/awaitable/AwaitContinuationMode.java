package org.pipelineframework.awaitable;

/**
 * Selects whether an await may keep an in-process live completion window.
 */
public enum AwaitContinuationMode {
    /**
     * An in-process transition may keep a live window when its await adapter supports one.
     */
    LIVE_IF_SUPPORTED,

    /**
     * A portable transition must suspend at the await boundary for coordinator continuation.
     */
    DURABLE_HANDOFF
}
