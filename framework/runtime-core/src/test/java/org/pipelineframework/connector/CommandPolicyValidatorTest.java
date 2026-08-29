package org.pipelineframework.connector;

import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CommandPolicyValidatorTest {

    private final ConnectorProviderDescriptor provider = new ConnectorProviderDescriptor(
        ConnectorProviderId.of("acme.search"),
        new ConnectorProviderVersion(1, 0),
        Optional.empty());

    private final ConnectorOperationDescriptor command = new ConnectorOperationDescriptor(
        "write.document", ConnectorOperationKind.COMMAND, 1, Optional.empty(),
        Optional.of(new CommandCapabilities(
            false, true, true, CommandExecutionPosture.AUTOMATED,
            CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, true, Set.of("ticket", "request"))));

    @Test
    void acceptsOnlyGuaranteesTheOperationAndCurrentRuntimeCanProvide() {
        assertDoesNotThrow(() -> CommandPolicyValidator.validate(provider, command, new CommandPolicy(
            false, true, true,
            Optional.of(CommandExecutionPosture.AUTOMATED),
            Optional.of(CommandMachineConfirmation.SUBMITTED), true)));
    }

    @Test
    void treatsUndeclaredPostureConservativelyAndRejectsPostureMismatch() {
        ConnectorOperationDescriptor undeclared = new ConnectorOperationDescriptor(
            "undeclared", ConnectorOperationKind.COMMAND, 1, Optional.empty(),
            Optional.of(CommandCapabilities.conservative()));
        CommandPolicy automated = new CommandPolicy(
            false, false, false,
            Optional.of(CommandExecutionPosture.AUTOMATED),
            Optional.empty(), false);

        IllegalArgumentException missing = assertThrows(
            IllegalArgumentException.class, () -> CommandPolicyValidator.validate(provider, undeclared, automated));
        assertEquals("command policy for provider acme.search operation undeclared requires command execution posture "
            + "AUTOMATED, but the provider declares UNSPECIFIED", missing.getMessage());

        CommandPolicy attended = new CommandPolicy(
            false, false, false,
            Optional.of(CommandExecutionPosture.ATTENDED),
            Optional.empty(), false);
        IllegalArgumentException mismatch = assertThrows(
            IllegalArgumentException.class, () -> CommandPolicyValidator.validate(provider, command, attended));
        assertEquals("command policy for provider acme.search operation write.document requires command execution posture "
            + "ATTENDED, but the provider declares AUTOMATED", mismatch.getMessage());
    }

    @Test
    void acceptsDeclaredRetryRedriveAndRejectsUnsupportedGuaranteesWithActionableDiagnostics() {
        IllegalArgumentException retry = assertThrows(IllegalArgumentException.class,
            () -> CommandPolicyValidator.validate(provider, command, new CommandPolicy(
                true, false, false, Optional.empty(), Optional.empty(), false)));
        assertEquals("command policy for provider acme.search operation write.document requires retry/redrive support",
            retry.getMessage());

        ConnectorOperationDescriptor retryCapable = new ConnectorOperationDescriptor(
            "write.retryable", ConnectorOperationKind.COMMAND, 1, Optional.empty(),
            Optional.of(new CommandCapabilities(
                true, true, true, CommandExecutionPosture.AUTOMATED,
                CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED, true, Set.of("ticket"))));
        assertDoesNotThrow(() -> CommandPolicyValidator.validate(provider, retryCapable, new CommandPolicy(
            true, true, false, Optional.empty(), Optional.empty(), false)));
    }

    @Test
    void filtersOnlyDeclaredDurableReferencesAtTheRuntimeBoundary() {
        assertEquals(Set.of("ticket", "request"), command.commandCapabilities().orElseThrow().durableReferenceKinds());
    }
}
