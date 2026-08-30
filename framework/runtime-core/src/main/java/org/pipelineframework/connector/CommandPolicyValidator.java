package org.pipelineframework.connector;

import java.util.Objects;

/**
 * Validates the guarantees a command step requests against static or live operation metadata.
 */
public final class CommandPolicyValidator {
    private CommandPolicyValidator() {
    }

    public static void validate(
        ConnectorProviderDescriptor provider,
        ConnectorOperationDescriptor operation,
        CommandPolicy policy
    ) {
        Objects.requireNonNull(provider, "provider descriptor must not be null");
        Objects.requireNonNull(operation, "operation descriptor must not be null");
        Objects.requireNonNull(policy, "command policy must not be null");
        if (!ConnectorOperationKind.COMMAND.equals(operation.kind())) {
            throw new IllegalArgumentException("command policy requires command operation " + operation.id());
        }
        String subject = "command policy for provider " + provider.id().value() + " operation " + operation.id();
        CommandCapabilities command = operation.commandCapabilities().orElse(CommandCapabilities.conservative());
        if (policy.requireRetryRedrive() && !command.retryRedriveSupported()) {
            throw new IllegalArgumentException(subject + " requires retry/redrive support");
        }
        if (policy.requireIdempotency() && !command.providerIdempotencySupported()) {
            throw new IllegalArgumentException(subject + " requires provider idempotency support");
        }
        if (policy.requireReconciliation() && !command.reconciliationSupported()) {
            throw new IllegalArgumentException(subject + " requires reconciliation support");
        }
        policy.requiredExecutionPosture().ifPresent(required -> {
            if (required == CommandExecutionPosture.UNSPECIFIED) {
                throw new IllegalArgumentException(subject + " must not require unspecified command execution posture");
            }
            if (command.executionPosture() != required) {
                throw new IllegalArgumentException(subject + " requires command execution posture " + required
                    + ", but the provider declares " + command.executionPosture());
            }
        });
        policy.minimumMachineConfirmation().ifPresent(required -> {
            if (!command.maximumMachineConfirmation().satisfies(required)) {
                throw new IllegalArgumentException(subject + " requires machine confirmation " + required
                    + ", but the provider declares " + command.maximumMachineConfirmation());
            }
        });
        if (policy.requireUserConfirmation() && !command.userConfirmationSupported()) {
            throw new IllegalArgumentException(subject + " requires user confirmation support");
        }
    }
}
