package org.pipelineframework.connector;

import java.util.Objects;

/**
 * Completion evidence. User acknowledgement is deliberately not machine verification.
 */
public record CommandConfirmation(CommandMachineConfirmation machineConfirmation, boolean userConfirmed) {
    public CommandConfirmation {
        machineConfirmation = Objects.requireNonNull(machineConfirmation, "machine confirmation must not be null");
    }

    public static CommandConfirmation none() {
        return new CommandConfirmation(CommandMachineConfirmation.NONE, false);
    }
}
