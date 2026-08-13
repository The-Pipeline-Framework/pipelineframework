package org.pipelineframework.command;

/**
 * Sanitized retryable failure exposed to the pipeline for a typed native command outcome.
 */
public final class CommandRetryableOutcomeException extends RuntimeException {
    private final String outcomeCode;

    public CommandRetryableOutcomeException(String outcomeCode) {
        super("command outcome failed_retryable: " + outcomeCode);
        this.outcomeCode = outcomeCode;
    }

    public String outcomeCode() {
        return outcomeCode;
    }
}
