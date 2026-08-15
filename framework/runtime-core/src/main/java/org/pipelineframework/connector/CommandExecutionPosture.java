package org.pipelineframework.connector;

/**
 * Whether a command operation is designed for unattended or attended execution.
 */
public enum CommandExecutionPosture {
    /** No execution-posture guarantee was declared. */
    UNSPECIFIED,
    /** The operation can complete without an attending user. */
    AUTOMATED,
    /** The operation may require an attending user. */
    ATTENDED
}
