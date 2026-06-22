package org.pipelineframework.command;

/**
 * Retryable failure raised when another execution attempt already owns the same command id.
 */
public class CommandInProgressException extends RuntimeException {
    public CommandInProgressException(String message) {
        super(message);
    }
}
