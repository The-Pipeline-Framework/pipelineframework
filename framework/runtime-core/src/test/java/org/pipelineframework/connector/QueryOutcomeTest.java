package org.pipelineframework.connector;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class QueryOutcomeTest {
    @Test
    void outcomeCodeDiagnosticDescribesLeadingCharacterAndLengthRules() {
        IllegalArgumentException leading = assertThrows(
            IllegalArgumentException.class, () -> new QueryOutcome.NotFound<>("1invalid"));
        IllegalArgumentException longCode = assertThrows(
            IllegalArgumentException.class, () -> new QueryOutcome.NotFound<>("a".repeat(129)));

        assertTrue(leading.getMessage().contains("start with a lowercase letter"), leading.getMessage());
        assertTrue(longCode.getMessage().contains("at most 128 characters"), longCode.getMessage());
    }
}
