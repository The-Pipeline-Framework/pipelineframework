package org.pipelineframework.connector;

/**
 * Machine-verifiable evidence strength for a completed command.
 */
public enum CommandMachineConfirmation {
    NONE,
    SUBMITTED,
    PROVIDER_ACKNOWLEDGED,
    READ_AFTER_WRITE_VERIFIED;

    public boolean satisfies(CommandMachineConfirmation required) {
        return ordinal() >= required.ordinal();
    }
}
