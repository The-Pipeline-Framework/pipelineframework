package org.pipelineframework.awaitable;

/**
 * Completion attempted for an interaction that is already terminal.
 */
public class AwaitInteractionTerminalException extends IllegalArgumentException implements AwaitCompletionAdmissionFailure {

    public AwaitInteractionTerminalException(String message) {
        super(message);
    }
}
