package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;

import org.junit.jupiter.api.Test;

class CommandEffectContractTest {

    @Test
    void reissueModelRequiresPurposeOccurrenceAndAuditReason() {
        assertThrows(IllegalArgumentException.class, () -> CommandAttemptAdmission.reissue(" "));
        assertThrows(IllegalArgumentException.class, () -> new CommandEffectAttemptRecord(
            "attempt-1", " ", 1, "execution-1", CommandAttemptPurpose.INITIAL,
            CommandEffectStatus.PENDING, Optional.empty(), null, null, Optional.empty(),
            Optional.empty(), 1L, 1L));
        assertThrows(IllegalArgumentException.class, () -> new CommandEffectAttemptRecord(
            "attempt-1", "occurrence-1", 1, "execution-1", CommandAttemptPurpose.REISSUE,
            CommandEffectStatus.PENDING, Optional.empty(), null, null, Optional.empty(),
            Optional.empty(), 1L, 1L));
    }
}
