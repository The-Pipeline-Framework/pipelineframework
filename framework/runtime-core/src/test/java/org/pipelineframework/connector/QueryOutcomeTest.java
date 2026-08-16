package org.pipelineframework.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class QueryOutcomeTest {

    @Test
    void exposesEveryTypedUnaryOutcome() {
        assertEquals("value", new QueryOutcome.Found<>("value").output());
        assertInstanceOf(QueryOutcome.NotFound.class, new QueryOutcome.NotFound<>("missing"));
        assertInstanceOf(QueryOutcome.TemporarilyUnavailable.class,
            new QueryOutcome.TemporarilyUnavailable<>("provider-busy"));
        assertInstanceOf(QueryOutcome.AuthenticationRequired.class,
            new QueryOutcome.AuthenticationRequired<>("authentication-required"));
        assertInstanceOf(QueryOutcome.TerminalFailure.class,
            new QueryOutcome.TerminalFailure<>("invalid-query"));
    }

    @Test
    void rejectsNullOutputsAndUnsafeCodes() {
        assertThrows(NullPointerException.class, () -> new QueryOutcome.Found<>(null));
        assertThrows(IllegalArgumentException.class, () -> new QueryOutcome.NotFound<>("Secret value"));
    }

    @Test
    void capabilitiesDefaultConservativelyAndValidateBounds() {
        assertEquals(QueryCacheability.LIVE_ONLY, QueryCapabilities.conservative().cacheability());
        assertEquals(Optional.empty(), QueryCapabilities.conservative().maximumCacheAge());
        assertThrows(IllegalArgumentException.class, () -> new QueryCapabilities(
            QueryCacheability.LIVE_ONLY, Optional.of(Duration.ofMinutes(1)), Optional.empty()));
        assertThrows(IllegalArgumentException.class, () -> new QueryCapabilities(
            QueryCacheability.CACHEABLE, Optional.empty(), Optional.of(Duration.ZERO)));
    }

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
