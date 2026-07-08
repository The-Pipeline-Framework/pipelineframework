package org.pipelineframework.orchestrator.controlplane;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.ExecutionRecord;

final class TerminalPublicationLedger {

    private final Supplier<Optional<ControlPlaneJournal>> journals;
    private final SegmentBoundaryJournalAppender appender;
    private final SegmentBoundaryFactFactory facts;

    TerminalPublicationLedger(
        Supplier<Optional<ControlPlaneJournal>> journals,
        SegmentBoundaryJournalAppender appender,
        SegmentBoundaryFactFactory facts
    ) {
        this.journals = journals;
        this.appender = appender;
        this.facts = facts;
    }

    Uni<TerminalPublicationClaim> claim(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        String publicationKind,
        long nowEpochMs
    ) {
        if (record == null) {
            return Uni.createFrom().item(TerminalPublicationClaim.untracked(null, null));
        }
        SegmentTerminalPublicationFacts publicationFacts = facts.terminalPublicationFacts(
            record,
            transitionKey,
            publicationKind);
        Optional<ControlPlaneJournal> journal = journals.get();
        if (journal.isEmpty()) {
            return Uni.createFrom().item(TerminalPublicationClaim.untracked(
                publicationFacts.publicationId(),
                publicationFacts.idempotencyKey()));
        }
        return claim(journal.get(), record, publicationFacts, nowEpochMs, 1);
    }

    Uni<Void> complete(
        ExecutionRecord<Object, Object> record,
        String transitionKey,
        String publicationKind,
        long nowEpochMs
    ) {
        if (record == null) {
            return Uni.createFrom().voidItem();
        }
        Optional<ControlPlaneJournal> journal = journals.get();
        if (journal.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        SegmentTerminalPublicationFacts publicationFacts = facts.terminalPublicationFacts(
            record,
            transitionKey,
            publicationKind);
        return appender.appendRequired(
                journal.get(),
                record.tenantId(),
                record.executionId(),
                List.of(publicationFacts.completed()),
                nowEpochMs)
            .replaceWithVoid();
    }

    private Uni<TerminalPublicationClaim> claim(
        ControlPlaneJournal journal,
        ExecutionRecord<Object, Object> record,
        SegmentTerminalPublicationFacts facts,
        long nowEpochMs,
        int attempt
    ) {
        return journal.projection(record.tenantId(), record.executionId())
            .onItem().transformToUni(projection -> {
                if (projection.terminalPublicationKeys().contains(facts.completed().factKey())) {
                    return Uni.createFrom().item(toClaim(
                        TerminalPublicationClaim.Status.ALREADY_COMPLETED,
                        facts));
                }
                if (projection.terminalPublicationPreparedKeys().contains(facts.prepared().factKey())) {
                    return Uni.createFrom().item(toClaim(
                        TerminalPublicationClaim.Status.PREPARED_RETRY,
                        facts));
                }
                return journal.append(
                        record.tenantId(),
                        record.executionId(),
                        projection.version(),
                        List.of(facts.prepared()),
                        nowEpochMs)
                    .onItem().transform(ignored -> toClaim(TerminalPublicationClaim.Status.CLAIMED, facts));
            })
            .onFailure(ControlPlaneAppendConflictException.class).recoverWithUni(failure -> {
                if (attempt >= SegmentBoundaryJournalAppender.MAX_APPEND_ATTEMPTS) {
                    return Uni.createFrom().failure(failure);
                }
                return claim(journal, record, facts, nowEpochMs, attempt + 1);
            });
    }

    private static TerminalPublicationClaim toClaim(
        TerminalPublicationClaim.Status status,
        SegmentTerminalPublicationFacts facts
    ) {
        return new TerminalPublicationClaim(
            status,
            facts.publicationId(),
            facts.idempotencyKey(),
            facts.prepared().factKey(),
            facts.completed().factKey());
    }
}
