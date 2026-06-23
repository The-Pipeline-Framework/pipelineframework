package org.pipelineframework.command;

/**
 * Durable effect lifecycle status for managed command steps.
 */
public enum CommandEffectStatus {
    PENDING,
    DISPATCHING,
    SUCCEEDED,
    FAILED_RETRYABLE,
    DLQ,
    FAILED
}
