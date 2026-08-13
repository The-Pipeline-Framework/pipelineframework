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
    CommandMachineConfirmation maximumMachineConfirmation,
    boolean userConfirmationSupported,
    Set<String> durableReferenceKinds
) {
    public CommandCapabilities {
        maximumMachineConfirmation = Objects.requireNonNull(
            maximumMachineConfirmation, "maximum machine confirmation must not be null");
        Objects.requireNonNull(durableReferenceKinds, "durable reference kinds must not be null");
        Set<String> validated = new LinkedHashSet<>();
        for (String kind : durableReferenceKinds) {
            validated.add(ConnectorProviderId.require(kind, "durable command reference kind"));
        }
        durableReferenceKinds = Set.copyOf(validated);
    }

    public static CommandCapabilities conservative() {
        return new CommandCapabilities(false, false, false, CommandMachineConfirmation.NONE, false, Set.of());
    }
}
