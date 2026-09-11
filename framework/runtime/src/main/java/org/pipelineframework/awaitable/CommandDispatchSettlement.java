package org.pipelineframework.awaitable;

/** Classification after ordinary Command effect evidence has been durably recorded. */
public enum CommandDispatchSettlement {
    SUCCEEDED, AMBIGUOUS, RETRYABLE, TERMINAL, USER_ACTION_REQUIRED
}
