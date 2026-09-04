package org.pipelineframework.command;

/** Why a particular dispatch attempt exists within a logical Command effect. */
public enum CommandAttemptPurpose {
    INITIAL,
    RETRY,
    REISSUE
}
