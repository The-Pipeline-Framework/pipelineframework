package org.pipelineframework.command;

/**
 * A concurrent writer won a conditional Command effect transition.
 */
public final class CommandEffectConflictException extends CommandEffectStoreException {

    public CommandEffectConflictException(String message, Throwable cause) {
        super(message, cause);
    }
}
