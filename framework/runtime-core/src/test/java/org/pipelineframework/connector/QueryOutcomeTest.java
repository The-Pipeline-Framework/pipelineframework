package org.pipelineframework.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import java.util.OptionalLong;
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

    @Test
    void tokenUsagePreservesIndependentProviderCounts() {
        QueryTokenUsage usage = new QueryTokenUsage(
            OptionalLong.of(12), OptionalLong.empty(), OptionalLong.of(99));

        assertEquals(OptionalLong.of(12), usage.inputTokens());
        assertEquals(OptionalLong.empty(), usage.outputTokens());
        assertEquals(OptionalLong.of(99), usage.totalTokens());
        assertThrows(IllegalArgumentException.class, () -> new QueryTokenUsage(
            OptionalLong.of(-1), OptionalLong.empty(), OptionalLong.empty()));
    }

    @Test
    void observationsConvertOriginWithoutChangingHistoricalMetadata() {
        QueryTokenUsage usage = new QueryTokenUsage(
            OptionalLong.of(4), OptionalLong.of(2), OptionalLong.empty());
        QueryObservation live = QueryObservation.live(
            Optional.of(usage), Optional.of("provider-model"), Optional.of("stop"));

        QueryObservation replay = live.asReplay();

        assertEquals(QueryObservationOrigin.LIVE_PROVIDER, live.origin());
        assertEquals(QueryObservationOrigin.CAPTURE_REPLAY, replay.origin());
        assertEquals(live.tokenUsage(), replay.tokenUsage());
        assertEquals(live.responseModel(), replay.responseModel());
        assertEquals(live.finishReason(), replay.finishReason());
    }

    @Test
    void legacyOutcomeConstructorsLeaveObservationAbsent() {
        assertTrue(new QueryOutcome.Found<>("value").observation().isEmpty());
        assertTrue(new QueryOutcome.NotFound<>("missing").observation().isEmpty());
        assertTrue(new QueryOutcome.TemporarilyUnavailable<>("busy").observation().isEmpty());
        assertTrue(new QueryOutcome.AuthenticationRequired<>("auth-required").observation().isEmpty());
        assertTrue(new QueryOutcome.TerminalFailure<>("failed").observation().isEmpty());
    }
}
