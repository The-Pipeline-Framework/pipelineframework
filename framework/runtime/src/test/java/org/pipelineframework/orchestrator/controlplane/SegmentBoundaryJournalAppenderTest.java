package org.pipelineframework.orchestrator.controlplane;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.ExecutionResultShape;

class SegmentBoundaryJournalAppenderTest {

    @Test
    void appendRequiredSkipsFactsAlreadyPresentInProjection() {
        InMemoryControlPlaneJournal journal = new InMemoryControlPlaneJournal();
        SegmentBoundaryJournalAppender appender = new SegmentBoundaryJournalAppender(() -> Optional.of(journal));
        ControlPlaneFact.RunSubmitted fact = runSubmitted("run-1");

        appender.appendRequired(journal, "tenant", "run-1", List.of(fact), 10L).await().indefinitely();
        appender.appendRequired(journal, "tenant", "run-1", List.of(fact), 11L).await().indefinitely();

        ControlPlaneProjection projection = journal.projection("tenant", "run-1").await().indefinitely();
        assertEquals(1L, projection.version());
        assertTrue(projection.factKeys().contains(fact.factKey()));
    }

    @Test
    void appendFactsOrThrowBuildFailureRetriesOptimisticConflict() {
        InMemoryControlPlaneJournal delegate = new InMemoryControlPlaneJournal();
        SegmentBoundaryJournalAppender appender =
            new SegmentBoundaryJournalAppender(() -> Optional.of(new ConflictOnceJournal(delegate)));

        appender.appendFactsOrThrowBuildFailure("tenant", "run-2", () -> List.of(runSubmitted("run-2")), 20L)
            .await().indefinitely();

        assertEquals(1L, delegate.projection("tenant", "run-2").await().indefinitely().version());
    }

    @Test
    void appendFactsOrThrowBuildFailurePropagatesBuildFailure() {
        SegmentBoundaryJournalAppender appender =
            new SegmentBoundaryJournalAppender(() -> Optional.of(new InMemoryControlPlaneJournal()));

        assertThrows(IllegalArgumentException.class, () -> appender.appendFactsOrThrowBuildFailure(
                "tenant",
                "run-build-error",
                () -> {
                    throw new IllegalArgumentException("bad facts");
                },
                30L)
            .await().indefinitely());
    }

    @Test
    void appendFactsBestEffortToleratesBuildFailure() {
        SegmentBoundaryJournalAppender appender =
            new SegmentBoundaryJournalAppender(() -> Optional.of(new InMemoryControlPlaneJournal()));

        appender.appendFactsBestEffort(
                "tenant",
                "run-best-effort",
                () -> {
                    throw new IllegalArgumentException("bad facts");
                },
                40L)
            .await().indefinitely();
    }

    private static ControlPlaneFact.RunSubmitted runSubmitted(String runId) {
        return new ControlPlaneFact.RunSubmitted(
            "tenant",
            runId,
            "key-" + runId,
            "pipeline",
            "contract",
            "release",
            ExecutionResultShape.SINGLE,
            runId + ":segment:0",
            0,
            -1,
            "input",
            999999L);
    }

    private static final class ConflictOnceJournal implements ControlPlaneJournal {
        private final InMemoryControlPlaneJournal delegate;
        private final AtomicBoolean conflict = new AtomicBoolean(true);

        private ConflictOnceJournal(InMemoryControlPlaneJournal delegate) {
            this.delegate = delegate;
        }

        @Override
        public Uni<ControlPlaneAppendResult> append(
            String tenantId,
            String runId,
            long expectedVersion,
            List<ControlPlaneFact> facts,
            long nowEpochMs
        ) {
            if (conflict.getAndSet(false)) {
                return Uni.createFrom().failure(new ControlPlaneAppendConflictException("synthetic conflict"));
            }
            return delegate.append(tenantId, runId, expectedVersion, facts, nowEpochMs);
        }

        @Override
        public Uni<ControlPlaneProjection> projection(String tenantId, String runId) {
            return delegate.projection(tenantId, runId);
        }

        @Override
        public Uni<List<DueSegment>> findDueSegments(long nowEpochMs, int limit) {
            return delegate.findDueSegments(nowEpochMs, limit);
        }

        @Override
        public Uni<List<DueBoundaryInteraction>> findTimedOutInteractions(long nowEpochMs, int limit) {
            return delegate.findTimedOutInteractions(nowEpochMs, limit);
        }
    }
}
