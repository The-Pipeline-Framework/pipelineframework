package org.pipelineframework.connector;

/**
 * Identities attached to one Command provider dispatch.
 *
 * @param commandId stable logical effect identity and provider idempotency key
 * @param attemptId identity of this individual dispatch attempt
 */
public record CommandDispatchIdentity(String commandId, String attemptId) {
    public CommandDispatchIdentity {
        if (commandId == null || commandId.isBlank()) {
            throw new IllegalArgumentException("commandId must not be blank");
        }
        if (attemptId == null || attemptId.isBlank()) {
            throw new IllegalArgumentException("attemptId must not be blank");
        }
    }
}
