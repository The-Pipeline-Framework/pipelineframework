package org.pipelineframework.command;

/**
 * A durable Command effect store could not read, validate, encode, or persist its authority record.
 *
 * <p>This failure is deliberately distinct from a provider outcome. Command execution must propagate
 * it without classifying it as an external-effect result.</p>
 */
public class CommandEffectStoreException extends RuntimeException {

    public CommandEffectStoreException(String message) {
        super(message);
    }

    public CommandEffectStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
