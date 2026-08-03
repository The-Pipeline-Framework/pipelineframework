package org.pipelineframework.awaitable;

/**
 * Identifies which side of a transition boundary publishes a terminal object output.
 */
public enum TerminalOutputOwnership {
    /** The transition worker publishes terminal output itself. */
    TRANSITION_WORKER,

    /** The coordinator receives the portable transition result and owns terminal publication. */
    COORDINATOR
}
