package org.pipelineframework.orchestrator;

/**
 * Explicit operator intent attached to one terminal-execution redrive.
 */
public enum ExecutionRedriveIntent {
    /** Re-run the failed execution boundary without authorizing a new Command effect attempt. */
    REPLAY,

    /** Authorize one deliberate retry attempt for the retained failed Command step. */
    RETRY_FAILED_COMMAND
}
