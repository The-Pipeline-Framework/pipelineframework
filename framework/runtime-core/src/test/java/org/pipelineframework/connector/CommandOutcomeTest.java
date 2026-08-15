package org.pipelineframework.connector;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class CommandOutcomeTest {

    @Test
    void requiresMeaningfulUserActionInstructions() {
        assertThrows(IllegalArgumentException.class,
            () -> new CommandOutcome.UserActionRequired<>("approval-required", " ", List.of()));
    }

    @Test
    void restrictsDurableReferencesToBoundedOpaqueIdentifiers() {
        assertThrows(IllegalArgumentException.class, () -> new CommandReference(
            "ticket", "https://provider.example/tickets/1?token=secret", CommandReferencePurpose.RECONCILIATION));
        assertThrows(IllegalArgumentException.class, () -> new CommandReference(
            "ticket", "ticket 1", CommandReferencePurpose.RECONCILIATION));
        assertThrows(IllegalArgumentException.class, () -> new CommandReference(
            "ticket", "T".repeat(CommandReference.MAX_VALUE_LENGTH + 1), CommandReferencePurpose.RECONCILIATION));

        CommandReference reference = new CommandReference(
            "ticket", "TKT-1", CommandReferencePurpose.RECONCILIATION);
        assertThrows(IllegalArgumentException.class, () -> new CommandOutcome.TerminalFailure<>(
            "failed", Collections.nCopies(CommandReference.MAX_REFERENCES_PER_OUTCOME + 1, reference)));
    }
}
