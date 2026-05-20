package org.pipelineframework.awaitable.spi;

import java.util.List;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitCreateResult;
import org.pipelineframework.awaitable.AwaitInteractionRecord;

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
     * Fetches one interaction by tenant and correlation id.
     *
     * @param tenantId tenant id
     * @param correlationId correlation id
     * @return matching interaction
     */
    Uni<Optional<AwaitInteractionRecord>> findByCorrelation(String tenantId, String correlationId);

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
