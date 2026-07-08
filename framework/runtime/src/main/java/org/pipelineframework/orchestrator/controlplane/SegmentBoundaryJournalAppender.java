package org.pipelineframework.orchestrator.controlplane;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;

final class SegmentBoundaryJournalAppender {

    private static final Logger LOG = Logger.getLogger(SegmentBoundaryJournalAppender.class);
    static final int MAX_APPEND_ATTEMPTS = 3;

    private final Supplier<Optional<ControlPlaneJournal>> journals;

    SegmentBoundaryJournalAppender(Supplier<Optional<ControlPlaneJournal>> journals) {
        this.journals = journals;
    }

    Uni<Void> appendFactsOrThrowBuildFailure(
        String tenantId,
        String runId,
        Supplier<List<ControlPlaneFact>> facts,
        long nowEpochMs
    ) {
        return appendFactsWithBestEffortPersistence(tenantId, runId, facts.get(), nowEpochMs);
    }

    Uni<Void> appendFactsBestEffort(
        String tenantId,
        String runId,
        Supplier<List<ControlPlaneFact>> facts,
        long nowEpochMs
    ) {
        try {
            return appendFactsWithBestEffortPersistence(tenantId, runId, facts.get(), nowEpochMs);
        } catch (IllegalArgumentException failure) {
            LOG.warnf(
                failure,
                "Failed building queue-async control-plane facts tenant=%s runId=%s",
                tenantId,
                runId);
            return Uni.createFrom().voidItem();
        }
    }

    Optional<ControlPlaneJournal> journal() {
        return journals.get();
    }

    Uni<ControlPlaneAppendResult> appendRequired(
        ControlPlaneJournal journal,
        String tenantId,
        String runId,
        List<ControlPlaneFact> facts,
        long nowEpochMs
    ) {
        return appendWithRetry(journal, tenantId, runId, List.copyOf(facts), nowEpochMs, 1);
    }

    private Uni<Void> appendFactsWithBestEffortPersistence(
        String tenantId,
        String runId,
        List<ControlPlaneFact> facts,
        long nowEpochMs
    ) {
        Optional<ControlPlaneJournal> journal = journal();
        if (journal.isEmpty() || facts == null || facts.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        return appendRequired(journal.get(), tenantId, runId, facts, nowEpochMs)
            .replaceWithVoid()
            .onFailure().recoverWithUni(failure -> {
                LOG.warnf(
                    failure,
                    "Failed appending queue-async control-plane facts tenant=%s runId=%s",
                    tenantId,
                    runId);
                return Uni.createFrom().voidItem();
            });
    }

    private Uni<ControlPlaneAppendResult> appendWithRetry(
        ControlPlaneJournal journal,
        String tenantId,
        String runId,
        List<ControlPlaneFact> facts,
        long nowEpochMs,
        int attempt
    ) {
        return journal.projection(tenantId, runId)
            .onItem().transformToUni(projection -> {
                List<ControlPlaneFact> missingFacts = facts.stream()
                    .filter(fact -> !projection.factKeys().contains(fact.factKey()))
                    .toList();
                if (missingFacts.isEmpty()) {
                    return Uni.createFrom().item(new ControlPlaneAppendResult(projection, List.of()));
                }
                return journal.append(tenantId, runId, projection.version(), missingFacts, nowEpochMs);
            })
            .onFailure(ControlPlaneAppendConflictException.class).recoverWithUni(failure -> {
                if (attempt >= MAX_APPEND_ATTEMPTS) {
                    return Uni.createFrom().failure(failure);
                }
                return appendWithRetry(journal, tenantId, runId, facts, nowEpochMs, attempt + 1);
            });
    }
}
