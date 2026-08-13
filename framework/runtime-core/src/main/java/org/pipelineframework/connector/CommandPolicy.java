package org.pipelineframework.connector;

import java.util.Objects;
import java.util.Optional;

/**
 * Opt-in guarantees required by one native command step.
 */
public record CommandPolicy(
    boolean requireRetryRedrive,
    boolean requireIdempotency,
    boolean requireReconciliation,
    Optional<ConnectorExecutionStyle> requiredExecutionStyle,
    Optional<ConnectorConcurrencyScope> requiredConcurrencyScope,
    Optional<CommandMachineConfirmation> minimumMachineConfirmation,
    boolean requireUserConfirmation
) {
    public CommandPolicy {
        requiredExecutionStyle = optional(requiredExecutionStyle, "required execution style");
        requiredConcurrencyScope = optional(requiredConcurrencyScope, "required concurrency scope");
        minimumMachineConfirmation = optional(minimumMachineConfirmation, "minimum machine confirmation");
    }

    public static CommandPolicy none() {
        return new CommandPolicy(false, false, false, Optional.empty(), Optional.empty(), Optional.empty(), false);
    }

    private static <T> Optional<T> optional(Optional<T> value, String label) {
        return Objects.requireNonNull(value, label + " must not be null");
    }
}
