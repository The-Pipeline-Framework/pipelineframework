package org.pipelineframework.orchestrator.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.InMemoryExecutionStateStore;
import org.pipelineframework.stream.OpaqueSourceCheckpoint;
import org.pipelineframework.stream.ResumableSourceDescriptor;

class StreamRegionProjectionTest {

    @Test
    void sourceRegionUsesBoundedCreditsWithoutAnAwaitCompletionSetOrAccumulatedOutput() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        StreamRegionRecord created = region(4);
        StreamRegionRecord stored = store.createStreamRegion(created).await().indefinitely().orElseThrow();

        StreamRegionRecord firstClaim = store.claimStreamRegion("tenant", "execution", "csv-input", "worker-a", 10L, 100L)
            .await().indefinitely().orElseThrow();
        StreamRegionRecord firstPage = store.recordStreamRegionPage(
                "tenant", "execution", "csv-input", firstClaim.version(), checkpoint("provider-token-25"), 2, false, 11L)
            .await().indefinitely().orElseThrow();

        assertEquals(2L, firstPage.nextLogicalOrdinal());
        assertEquals(2, firstPage.outstandingCredits());
        assertEquals(2, firstPage.availableCredits());
        assertFalse(firstPage.sourceSealed());
        assertEquals(stored.source(), firstPage.source());

        StreamRegionRecord secondClaim = store.claimStreamRegion("tenant", "execution", "csv-input", "worker-b", 11L, 100L)
            .await().indefinitely().orElseThrow();
        StreamRegionRecord sealed = store.recordStreamRegionPage(
                "tenant", "execution", "csv-input", secondClaim.version(), checkpoint("provider-token-eof"), 2, true, 12L)
            .await().indefinitely().orElseThrow();

        assertTrue(sealed.sourceSealed());
        assertEquals(Optional.of(4L), sealed.finalOrdinal());
        assertEquals(4, sealed.outstandingCredits());
        assertEquals(StreamRegionStatus.SOURCE_SEALED, sealed.status());
    }

    @Test
    void scalarContinuationCreditsCanCompleteASealedRegionWithoutReinterpretingProviderCheckpoint() {
        InMemoryExecutionStateStore store = new InMemoryExecutionStateStore();
        store.createStreamRegion(region(2)).await().indefinitely();
        StreamRegionRecord claimed = store.claimStreamRegion("tenant", "execution", "csv-input", "worker", 10L, 100L)
            .await().indefinitely().orElseThrow();
        StreamRegionRecord sealed = store.recordStreamRegionPage(
                "tenant", "execution", "csv-input", claimed.version(), checkpoint("opaque:provider:state"), 2, true, 11L)
            .await().indefinitely().orElseThrow();

        StreamRegionRecord oneApplied = store.releaseStreamRegionCredit(
                "tenant", "execution", "csv-input", sealed.version(), 12L)
            .await().indefinitely().orElseThrow();
        StreamRegionRecord completed = store.releaseStreamRegionCredit(
                "tenant", "execution", "csv-input", oneApplied.version(), 13L)
            .await().indefinitely().orElseThrow();

        assertEquals("opaque:provider:state", completed.checkpoint().value().orElseThrow());
        assertEquals(0, completed.outstandingCredits());
        assertEquals(StreamRegionStatus.COMPLETED, completed.status());
    }

    @Test
    void emptyFiniteSourceSealsAndCompletesWithoutInventingAnAwaitAggregate() {
        StreamRegionRecord region = region(2);

        StreamRegionRecord completed = region.recordPage(checkpoint("opaque:eof"), 0, true, 11L);

        assertTrue(completed.sourceSealed());
        assertEquals(Optional.of(0L), completed.finalOrdinal());
        assertEquals(StreamRegionStatus.COMPLETED, completed.status());
        assertEquals(Long.MAX_VALUE, completed.nextDueEpochMs());
    }

    private static StreamRegionRecord region(int window) {
        return new StreamRegionRecord(
            "tenant", "execution", "csv-input",
            new ResumableSourceDescriptor("opencsv", "csv-payments-input", "sha256:source-binding"),
            OpaqueSourceCheckpoint.initial(), 0L, 0, window, StreamRegionStatus.ACTIVE, Optional.empty(),
            0L, "", 0L, 10L, 1L, 1L, 100L);
    }

    private static OpaqueSourceCheckpoint checkpoint(String value) {
        return new OpaqueSourceCheckpoint(Optional.of(value));
    }
}
