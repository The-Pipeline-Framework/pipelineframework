package org.pipelineframework.connector;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Command-family guarantees an operation can prove.
 */
public record CommandCapabilities(
    boolean retryRedriveSupported,
    boolean providerIdempotencySupported,
    boolean reconciliationSupported,
    CommandExecutionPosture executionPosture,
    CommandMachineConfirmation maximumMachineConfirmation,
    boolean userConfirmationSupported,
    Set<String> durableReferenceKinds
) {
    public CommandCapabilities {
        executionPosture = Objects.requireNonNull(executionPosture, "command execution posture must not be null");
        maximumMachineConfirmation = Objects.requireNonNull(
            maximumMachineConfirmation, "maximum machine confirmation must not be null");
        Objects.requireNonNull(durableReferenceKinds, "durable reference kinds must not be null");
        Set<String> validated = new LinkedHashSet<>();
        for (String kind : durableReferenceKinds) {
            validated.add(ConnectorProviderId.require(kind, "durable command reference kind"));
        }
        durableReferenceKinds = Set.copyOf(validated);
    }

    /**
     * Compatibility constructor for capability declarations created before execution posture was introduced.
     */
    public CommandCapabilities(
        boolean retryRedriveSupported,
        boolean providerIdempotencySupported,
        boolean reconciliationSupported,
        CommandMachineConfirmation maximumMachineConfirmation,
        boolean userConfirmationSupported,
        Set<String> durableReferenceKinds
    ) {
        this(
            retryRedriveSupported,
            providerIdempotencySupported,
            reconciliationSupported,
            CommandExecutionPosture.UNSPECIFIED,
            maximumMachineConfirmation,
            userConfirmationSupported,
            durableReferenceKinds);
    }

    public static CommandCapabilities conservative() {
        return new CommandCapabilities(
            false,
            false,
            false,
            CommandExecutionPosture.UNSPECIFIED,
            CommandMachineConfirmation.NONE,
            false,
            Set.of());
    }
}
