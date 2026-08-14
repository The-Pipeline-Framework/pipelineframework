package org.pipelineframework.command;

/**
 * Sanitized retryable failure exposed to the pipeline for a typed native command outcome.
 */
public final class CommandRetryableOutcomeException extends RuntimeException {
    private final String outcomeCode;

    public CommandRetryableOutcomeException(String outcomeCode) {
        super("command outcome failed_retryable: " + requireCode(outcomeCode));
        this.outcomeCode = requireCode(outcomeCode);
    }

    public String outcomeCode() {
        return outcomeCode;
    }

    private static String requireCode(String outcomeCode) {
        if (outcomeCode == null || outcomeCode.isBlank()) {
            throw new IllegalArgumentException("command outcome code must not be blank");
        }
        return outcomeCode;
    }
}
