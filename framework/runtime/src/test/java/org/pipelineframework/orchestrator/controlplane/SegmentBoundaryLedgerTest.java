package org.pipelineframework.orchestrator.controlplane;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStatus;

class SegmentBoundaryLedgerTest {

    @Test
    void recordRunSubmittedIsIdempotentByFactKey() {
        InMemoryControlPlaneJournal journal = new InMemoryControlPlaneJournal();
        SegmentBoundaryLedger ledger = new SegmentBoundaryLedger(journal);
        ExecutionRecord<Object, Object> record = record("run-1");
        ExecutionCreateCommand command = command(record);

        ledger.recordRunSubmitted(new CreateExecutionResult(record, false), command, 10L).await().indefinitely();
        ledger.recordRunSubmitted(new CreateExecutionResult(record, false), command, 11L).await().indefinitely();

        ControlPlaneProjection projection = journal.projection("tenant", "run-1").await().indefinitely();
        assertEquals(1L, projection.version());
        assertEquals(PipelineRunStatus.ACCEPTED, projection.status());
        assertTrue(projection.factKeys().contains("run-submitted:tenant:run-1"));
    }

    @Test
    void appendRetriesWhenProjectionVersionChangesBeforeAppend() {
        InMemoryControlPlaneJournal delegate = new InMemoryControlPlaneJournal();
        SegmentBoundaryLedger ledger = new SegmentBoundaryLedger(new ConflictOnceJournal(delegate));
        ExecutionRecord<Object, Object> record = record("run-2");

        ledger.recordSegmentAttemptStarted(record, "run-2:0:0", 20L).await().indefinitely();

        ControlPlaneProjection projection = delegate.projection("tenant", "run-2").await().indefinitely();
        assertEquals(1L, projection.version());
        assertTrue(projection.factKeys().contains("segment-attempt-started:run-2:0:0"));
    }

    @Test
    void duplicateSegmentAttemptAppendIsNoOp() {
        InMemoryControlPlaneJournal journal = new InMemoryControlPlaneJournal();
        SegmentBoundaryLedger ledger = new SegmentBoundaryLedger(journal);
        ExecutionRecord<Object, Object> record = record("run-3");

        ledger.recordSegmentAttemptStarted(record, "run-3:0:0", 30L).await().indefinitely();
        ledger.recordSegmentAttemptStarted(record, "run-3:0:0", 31L).await().indefinitely();

        ControlPlaneProjection projection = journal.projection("tenant", "run-3").await().indefinitely();
        assertEquals(1L, projection.version());
        assertEquals(1, projection.attempts().size());
    }

    @Test
    void segmentFactCanBeRecordedBeforeRunProjectionExists() {
        InMemoryControlPlaneJournal journal = new InMemoryControlPlaneJournal();
        SegmentBoundaryLedger ledger = new SegmentBoundaryLedger(journal);

        ledger.recordSegmentAttemptStarted(record("run-4"), "run-4:0:0", 40L).await().indefinitely();

        ControlPlaneProjection projection = journal.projection("tenant", "run-4").await().indefinitely();
        assertFalse(projection.run().isPresent());
        assertTrue(projection.attempts().containsKey("run-4:0:0"));
    }

    private static ExecutionCreateCommand command(ExecutionRecord<Object, Object> record) {
        return new ExecutionCreateCommand(
            record.tenantId(),
            record.executionKey(),
            record.pipelineId(),
            record.contractVersion(),
            record.releaseVersion(),
            "input",
            record.resultShape(),
            10L,
            record.ttlEpochS());
    }

    private static ExecutionRecord<Object, Object> record(String executionId) {
        return new ExecutionRecord<>(
            "tenant",
            executionId,
            "key-" + executionId,
            "pipeline",
            "contract",
            "release",
            ExecutionResultShape.SINGLE,
            ExecutionStatus.QUEUED,
            0L,
            0,
            0,
            null,
            0L,
            0L,
            null,
            "input",
            null,
            null,
            null,
            null,
            1L,
            1L,
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
