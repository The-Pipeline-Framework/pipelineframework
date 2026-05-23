package org.pipelineframework.awaitable;

/**
 * Completion attempted with an invalid or expired resume token.
 */
public class AwaitResumeTokenRejectedException extends IllegalArgumentException implements AwaitCompletionAdmissionFailure {

    public AwaitResumeTokenRejectedException(String message) {
        super(message);
    }

    public AwaitResumeTokenRejectedException(String message, Throwable cause) {
        super(message, cause);
    }
}
