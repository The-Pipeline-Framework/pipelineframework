package org.pipelineframework.awaitable;

import jakarta.ws.rs.NotFoundException;

/**
 * Completion attempted for an interaction that is not present in durable state.
 */
public class AwaitInteractionNotFoundException extends NotFoundException implements AwaitCompletionAdmissionFailure {

    public AwaitInteractionNotFoundException(String message) {
        super(message);
    }
}
