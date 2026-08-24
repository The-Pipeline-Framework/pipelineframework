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
        ConnectorExecutionCapabilities execution = provider.executionCapabilities()
            .orElse(ConnectorExecutionCapabilities.conservative());
        if (execution.executionStyle() == ConnectorExecutionStyle.BLOCKING) {
            throw new IllegalArgumentException(subject
                + " declares blocking execution, which requires framework-managed execution deferred to #577");
        }
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
        policy.requiredExecutionStyle().ifPresent(required -> {
            requireSupportedExecutionStyle(required, subject);
            if (execution.executionStyle() != required) {
                throw new IllegalArgumentException(subject + " requires execution style " + required
                    + ", but the provider declares " + execution.executionStyle());
            }
        });
        policy.requiredConcurrencyScope().ifPresent(required -> {
            requireSupportedConcurrencyScope(required, subject);
            if (execution.concurrencyScope() != required) {
                throw new IllegalArgumentException(subject + " requires concurrency scope " + required
                    + ", but the provider declares " + execution.concurrencyScope());
            }
        });
    }

    private static void requireSupportedExecutionStyle(ConnectorExecutionStyle style, String subject) {
        if (style == ConnectorExecutionStyle.BLOCKING) {
            throw new IllegalArgumentException(subject + " requires framework-managed blocking execution, deferred to #577");
        }
        if (style == ConnectorExecutionStyle.UNSPECIFIED) {
            throw new IllegalArgumentException(subject + " must not require unspecified execution style");
        }
    }

    private static void requireSupportedConcurrencyScope(ConnectorConcurrencyScope scope, String subject) {
        if (scope == ConnectorConcurrencyScope.PROVIDER_SCOPED
            || scope == ConnectorConcurrencyScope.CONNECTION_SCOPED
            || scope == ConnectorConcurrencyScope.OPERATION_SCOPED) {
            throw new IllegalArgumentException(subject + " requires framework-managed concurrency, deferred to #577");
        }
        if (scope == ConnectorConcurrencyScope.UNSPECIFIED) {
            throw new IllegalArgumentException(subject + " must not require unspecified concurrency scope");
        }
    }
}
