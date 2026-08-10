package org.pipelineframework.awaitable.spi;

import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitCreateResult;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.orchestrator.stream.StreamRegionPageCommit;
import org.pipelineframework.orchestrator.stream.StreamRegionRecord;

/**
 * Control-plane persistence SPI for await interactions.
 */
public interface AwaitInteractionStore {

    /**
     * Provider name used for configuration-based selection.
     *
     * @return provider name
     */
    default String providerName() {
        return "memory";
    }

    /**
     * Provider priority used when multiple stores are available.
     *
     * @return provider priority
     */
    default int priority() {
        return 0;
    }

    /**
     * Creates a new interaction or returns an active duplicate for the same idempotency key.
     *
     * @param command create command
     * @return create result
     */
    Uni<AwaitCreateResult> createOrGet(AwaitCreateCommand command);

    /**
     * Fetches one interaction by tenant and interaction id.
     *
     * @param tenantId tenant id
     * @param interactionId interaction id
     * @return matching interaction
     */
    Uni<Optional<AwaitInteractionRecord>> get(String tenantId, String interactionId);

    /**
     * Imports an interaction created by a transition worker. Existing records win so the operation is idempotent.
     */
    Uni<AwaitInteractionRecord> importRecord(AwaitInteractionRecord record);

    /**
     * Fetches one interaction by tenant and correlation id.
     *
     * @param tenantId tenant id
     * @param correlationId correlation id
     * @return matching interaction
     */
    Uni<Optional<AwaitInteractionRecord>> findByCorrelation(String tenantId, String correlationId);

    /**
     * Fetches interactions that belong to one await unit.
     *
     * @param tenantId tenant id
     * @param unitId await unit id
     * @return matching interactions
     */
    Uni<List<AwaitInteractionRecord>> findByUnit(
        String tenantId,
        String unitId);

    /**
     * Claims an interaction for dispatch. Only WAITING interactions may be claimed.
     */
    Uni<Optional<AwaitInteractionRecord>> markDispatching(
        String tenantId,
        String interactionId,
        long expectedVersion,
        long nowEpochMs);

    /**
     * Marks a claimed interaction as dispatched and stores adapter metadata.
     * Only DISPATCHING interactions may be completed as dispatched.
     *
     * @param tenantId tenant id
     * @param interactionId interaction id
     * @param expectedVersion expected version
     * @param transportMetadata transport metadata captured after dispatch
     * @param nowEpochMs current time
     * @return updated record when the transition succeeds
     */
    Uni<Optional<AwaitInteractionRecord>> markDispatched(
        String tenantId,
        String interactionId,
        long expectedVersion,
        java.util.Map<String, Object> transportMetadata,
        long nowEpochMs);

    /**
     * Accepts a correlated completion.
     *
     * @param command completion command
     * @return completion result
     */
    Uni<AwaitCompletionResult> complete(AwaitCompletionCommand command);

    /**
     * Makes an admitted item completion independently schedulable. This is idempotent: an
     * already-ready, claimed, retried, or applied continuation is returned unchanged.
     */
    Uni<Optional<AwaitInteractionRecord>> activateContinuationIfEligible(
        String tenantId,
        String interactionId,
        long expectedVersion,
        long nowEpochMs);

    /**
     * Claims one due continuation by interaction identity. Recovery and duplicate delivery use
     * the same conditional claim; no process-local claim is authoritative.
     */
    Uni<Optional<AwaitInteractionRecord>> claimDueContinuation(
        String tenantId,
        String interactionId,
        String leaseOwner,
        long nowEpochMs,
        long leaseMs);

    /** Returns bounded due continuation candidates for coordinator recovery sweeps. */
    Uni<List<AwaitInteractionRecord>> findDueContinuations(long nowEpochMs, int limit);

    /**
     * Returns bounded stream-linked interactions whose atomically committed source page has not
     * yet reached provider dispatch. This is the durable recovery authority for a coordinator
     * loss between page commit and dispatch; it deliberately excludes ordinary await rows.
     */
    Uni<List<AwaitInteractionRecord>> findDueStreamInteractionDispatches(long nowEpochMs, int limit);

    /** Persists a retry due time after a failed claimed continuation attempt. */
    Uni<Optional<AwaitInteractionRecord>> rescheduleContinuation(
        String tenantId,
        String interactionId,
        long expectedVersion,
        long nextDueEpochMs,
        long nowEpochMs);

    /** Records the scalar suffix result exactly once and makes the continuation applied. */
    Uni<Optional<AwaitInteractionRecord>> completeContinuation(
        String tenantId,
        String interactionId,
        long expectedVersion,
        Object outputPayload,
        long nowEpochMs);

    /**
     * Applies a claimed item continuation and returns exactly one credit to its linked producer
     * region. Durable implementations must make the two mutations one atomic operation; the
     * CLAIMED-to-APPLIED interaction transition is the sole idempotency token.
     */
    default Uni<Optional<AwaitInteractionRecord>> completeContinuationAndReleaseStreamCredit(
        String tenantId,
        String interactionId,
        long expectedVersion,
        String leaseOwner,
        Object outputPayload,
        long nowEpochMs
    ) {
        return Uni.createFrom().failure(new UnsupportedOperationException(
            "Atomic stream-credit release is not supported by this await interaction store"));
    }

    /** Atomically persists a bounded source page and advances its producer-owned stream region. */
    default Uni<Optional<StreamRegionRecord>> materializeStreamRegionPage(StreamRegionPageCommit commit) {
        return Uni.createFrom().failure(new UnsupportedOperationException(
            "Atomic stream-page materialisation is not supported by this await interaction store"));
    }

    /**
     * Marks an interaction as failed.
     */
    Uni<Optional<AwaitInteractionRecord>> fail(
        String tenantId,
        String interactionId,
        long expectedVersion,
        String reason,
        long nowEpochMs);

    /**
     * Marks an interaction as cancelled.
     */
    Uni<Optional<AwaitInteractionRecord>> cancel(
        String tenantId,
        String interactionId,
        long expectedVersion,
        String reason,
        long nowEpochMs);

    /**
     * Marks an interaction as timed out.
     */
    Uni<Optional<AwaitInteractionRecord>> markTimedOut(
        String tenantId,
        String interactionId,
        long expectedVersion,
        long nowEpochMs);

    /**
     * Returns active interactions whose deadline has passed.
     */
    Uni<List<AwaitInteractionRecord>> findTimedOut(long nowEpochMs, int limit);

    /**
     * Returns active interactions matching query filters.
     */
    Uni<List<AwaitInteractionRecord>> queryPending(
        String tenantId,
        String assignee,
        String group,
        String stepId,
        int limit);
}
