package org.pipelineframework.orchestrator.controlplane;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.TransitionAwaitSuspension;

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

    @Test
    void terminalPublicationClaimIsPreparedThenCompletedByFactKey() {
        InMemoryControlPlaneJournal journal = new InMemoryControlPlaneJournal();
        SegmentBoundaryLedger ledger = new SegmentBoundaryLedger(journal);
        ExecutionRecord<Object, Object> record = record("run-terminal");

        TerminalPublicationClaim claim = ledger.claimTerminalPublication(
                record,
                "run-terminal:0:0",
                "object-publish",
                50L)
            .await().indefinitely();
        ledger.completeTerminalPublication(record, "run-terminal:0:0", "object-publish", 51L)
            .await().indefinitely();

        ControlPlaneProjection projection = journal.projection("tenant", "run-terminal").await().indefinitely();
        assertEquals(TerminalPublicationClaim.Status.CLAIMED, claim.status());
        assertTrue(projection.terminalPublicationPreparedKeys().contains(claim.preparedFactKey()));
        assertTrue(projection.terminalPublicationKeys().contains(claim.completedFactKey()));
    }

    @Test
    void duplicateTerminalPublicationClaimReturnsPreparedRetryBeforeCompletion() {
        InMemoryControlPlaneJournal journal = new InMemoryControlPlaneJournal();
        SegmentBoundaryLedger ledger = new SegmentBoundaryLedger(journal);
        ExecutionRecord<Object, Object> record = record("run-terminal-retry");

        TerminalPublicationClaim first = ledger.claimTerminalPublication(
                record,
                "run-terminal-retry:0:0",
                "object-publish",
                60L)
            .await().indefinitely();
        TerminalPublicationClaim second = ledger.claimTerminalPublication(
                record,
                "run-terminal-retry:0:0",
                "object-publish",
                61L)
            .await().indefinitely();

        ControlPlaneProjection projection = journal.projection("tenant", "run-terminal-retry").await().indefinitely();
        assertEquals(TerminalPublicationClaim.Status.CLAIMED, first.status());
        assertEquals(TerminalPublicationClaim.Status.PREPARED_RETRY, second.status());
        assertEquals(first.idempotencyKey(), second.idempotencyKey());
        assertEquals(1L, projection.version());
    }

    @Test
    void untrackedTerminalPublicationClaimStillCarriesStableIdentifiers() {
        SegmentBoundaryLedger ledger = new SegmentBoundaryLedger((ControlPlaneJournal) null);
        ExecutionRecord<Object, Object> record = record("run-terminal-untracked");

        TerminalPublicationClaim claim = ledger.claimTerminalPublication(
                record,
                "run-terminal-untracked:0:0",
                "object-publish",
                65L)
            .await().indefinitely();

        assertEquals(TerminalPublicationClaim.Status.UNTRACKED, claim.status());
        assertEquals("run-terminal-untracked:terminal-output:object-publish", claim.publicationId());
        assertEquals("run-terminal-untracked:terminal-output:object-publish:terminal-publication",
            claim.idempotencyKey());
    }

    @Test
    void terminalPublicationClaimSkipsAlreadyCompletedPublication() {
        InMemoryControlPlaneJournal journal = new InMemoryControlPlaneJournal();
        SegmentBoundaryLedger ledger = new SegmentBoundaryLedger(journal);
        ExecutionRecord<Object, Object> record = record("run-terminal-completed");

        ledger.claimTerminalPublication(record, "run-terminal-completed:0:0", "object-publish", 70L)
            .await().indefinitely();
        ledger.completeTerminalPublication(record, "run-terminal-completed:0:0", "object-publish", 71L)
            .await().indefinitely();

        TerminalPublicationClaim duplicate = ledger.claimTerminalPublication(
                record,
                "run-terminal-completed:0:0",
                "object-publish",
                72L)
            .await().indefinitely();

        assertEquals(TerminalPublicationClaim.Status.ALREADY_COMPLETED, duplicate.status());
    }

    @Test
    void terminalPublicationClaimRetriesBoundedOccConflict() {
        InMemoryControlPlaneJournal delegate = new InMemoryControlPlaneJournal();
        SegmentBoundaryLedger ledger = new SegmentBoundaryLedger(new ConflictOnceJournal(delegate));
        ExecutionRecord<Object, Object> record = record("run-terminal-conflict");

        TerminalPublicationClaim claim = ledger.claimTerminalPublication(
                record,
                "run-terminal-conflict:0:0",
                "object-publish",
                80L)
            .await().indefinitely();

        assertEquals(TerminalPublicationClaim.Status.CLAIMED, claim.status());
        assertTrue(delegate.projection("tenant", "run-terminal-conflict")
            .await().indefinitely()
            .terminalPublicationPreparedKeys()
            .contains(claim.preparedFactKey()));
    }

    @Test
    void requiredBoundaryInteractionFactsFailFastWhenPayloadCannotBeFrozen() {
        SegmentBoundaryLedger ledger = new SegmentBoundaryLedger(new InMemoryControlPlaneJournal());
        ExecutionRecord<Object, Object> record = record("run-5");
        AwaitUnitRecord unit = new AwaitUnitRecord(
            "tenant",
            "unit-1",
            "run-5",
            "AwaitPaymentProvider",
            2,
            "ONE_TO_ONE",
            1L,
            AwaitUnitStatus.WAITING_EXTERNAL,
            null,
            1,
            0,
            Set.of(),
            true,
            1L,
            1L,
            999999L);
        AwaitInteractionRecord interaction = new AwaitInteractionRecord(
            "tenant",
            "run-5",
            "AwaitPaymentProvider",
            2,
            String.class.getName(),
            "interaction-1",
            "corr-1",
            "cause-1",
            "idem-1",
            1L,
            AwaitInteractionStatus.WAITING,
            new SelfReferentialPayload(),
            null,
            "unit-1",
            0,
            "user-1",
            null,
            null,
            "kafka",
            Map.of(),
            999999L,
            1L,
            1L,
            999999L);

        assertThrows(IllegalArgumentException.class, () -> ledger.recordSegmentSuspended(
                record,
                "run-5:2:0",
                new TransitionAwaitSuspension("tenant", "run-5", "unit-1", 2, unit, List.of(interaction)),
                50L)
            .await().indefinitely());
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

    private static final class SelfReferentialPayload {
        @SuppressWarnings("unused")
        public final Object self = this;
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
