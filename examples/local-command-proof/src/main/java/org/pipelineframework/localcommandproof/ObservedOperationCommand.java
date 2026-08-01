package org.pipelineframework.localcommandproof;

/**
 * Typed input for the local command proof fixture.
 */
public record ObservedOperationCommand(String operationId, Behavior behavior) {
    public ObservedOperationCommand {
        if (operationId == null || operationId.isBlank()) {
            throw new IllegalArgumentException("operationId must not be blank");
        }
        behavior = behavior == null ? Behavior.SUCCESS : behavior;
    }

    public enum Behavior {
        SUCCESS,
        BLOCKING_SUCCESS,
        RETRYABLE_FAILURE,
        NON_RETRYABLE_FAILURE
    }
}
