package org.pipelineframework.connector;

import java.util.Objects;

/**
 * Model-safe Command semantics. Operational retry, reconciliation, idempotency and durable-reference
 * mechanics deliberately remain outside the callable projection.
 */
public record CallableCommandCapabilities(
    CommandExecutionPosture executionPosture,
    CommandMachineConfirmation maximumMachineConfirmation,
    boolean userConfirmationSupported
) {
    public CallableCommandCapabilities {
        executionPosture = Objects.requireNonNull(executionPosture, "command execution posture must not be null");
        maximumMachineConfirmation = Objects.requireNonNull(
            maximumMachineConfirmation, "maximum machine confirmation must not be null");
    }

    public static CallableCommandCapabilities from(CommandCapabilities capabilities) {
        Objects.requireNonNull(capabilities, "command capabilities must not be null");
        return new CallableCommandCapabilities(
            capabilities.executionPosture(),
            capabilities.maximumMachineConfirmation(),
            capabilities.userConfirmationSupported());
    }
}
