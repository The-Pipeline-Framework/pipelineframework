package org.pipelineframework.connector;

/**
 * Identities attached to one Command provider dispatch.
 *
 * @param commandId stable logical effect identity
 * @param occurrenceId stable identity and provider idempotency key for one intentional effect occurrence
 * @param attemptId identity of this individual dispatch attempt
 */
public record CommandDispatchIdentity(String commandId, String occurrenceId, String attemptId) {
    public CommandDispatchIdentity(String commandId, String attemptId) {
        this(commandId, commandId, attemptId);
    }

    public CommandDispatchIdentity {
        if (commandId == null || commandId.isBlank()) {
            throw new IllegalArgumentException("commandId must not be blank");
        }
        if (attemptId == null || attemptId.isBlank()) {
            throw new IllegalArgumentException("attemptId must not be blank");
        }
        if (occurrenceId == null || occurrenceId.isBlank()) {
            throw new IllegalArgumentException("occurrenceId must not be blank");
        }
    }

    /** Stable provider idempotency key for this intentional effect occurrence. */
    public String providerIdempotencyKey() {
        return occurrenceId;
    }
}
