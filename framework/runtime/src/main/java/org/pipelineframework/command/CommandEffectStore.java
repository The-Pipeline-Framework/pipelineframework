package org.pipelineframework.command;

import java.util.Optional;

import io.smallrye.mutiny.Uni;

/**
 * Stores managed command effect state.
 *
 * Implementations own the durable command-id index and should encode persistence,
 * optimistic-lock, and connectivity failures as failed {@link Uni} items. Transition
 * methods are expected to fail on missing command ids or illegal state transitions;
 * callers do not perform multi-writer conflict resolution.
 *
 * Implementations must preserve the output type recorded by {@link #markSucceeded}
 * so replayed command results can be returned without lossy casts or schema drift.
 */
public interface CommandEffectStore {
    /**
     * Returns the current effect record for the tenant and command id, or empty when no
     * effect has been recorded yet.
     */
    Uni<Optional<CommandEffectRecord>> find(String tenantId, String commandId);

    /**
     * Creates the initial pending record for a command request. Implementations should
     * fail the returned {@link Uni} when the command id already exists.
     */
    Uni<CommandEffectRecord> createPending(CommandRequest<?> request, long nowEpochMs);

    /**
     * Marks an existing pending command as dispatching. Retrying this transition may be
     * accepted only when the stored state already reflects the same dispatch.
     */
    Uni<CommandEffectRecord> markDispatching(String tenantId, String commandId, long nowEpochMs);

    /**
     * Records the successful command output. The stored output type must remain compatible
     * with the command step output type for duplicate replay.
     */
    Uni<CommandEffectRecord> markSucceeded(String tenantId, String commandId, Object output, long nowEpochMs);

    /**
     * Records a retryable command failure. Implementations should retain enough error
     * detail for operators to classify and retry the effect.
     */
    Uni<CommandEffectRecord> markFailed(String tenantId, String commandId, Throwable failure, long nowEpochMs);

    /**
     * Records a terminal command failure that should be routed to dead-letter handling.
     */
    Uni<CommandEffectRecord> markDlq(String tenantId, String commandId, Throwable failure, long nowEpochMs);
}
