package org.pipelineframework.command;

import java.util.Optional;

import io.smallrye.mutiny.Uni;

/**
 * Stores managed command effect state.
 *
 * Implementations must preserve the output type recorded by {@link #markSucceeded}
 * so replayed command results can be returned without lossy casts or schema drift.
 */
public interface CommandEffectStore {
    Uni<Optional<CommandEffectRecord>> find(String tenantId, String commandId);

    Uni<CommandEffectRecord> createPending(CommandRequest<?> request, long nowEpochMs);

    Uni<CommandEffectRecord> markDispatching(String tenantId, String commandId, long nowEpochMs);

    Uni<CommandEffectRecord> markSucceeded(String tenantId, String commandId, Object output, long nowEpochMs);

    Uni<CommandEffectRecord> markFailed(String tenantId, String commandId, Throwable failure, long nowEpochMs);

    Uni<CommandEffectRecord> markDlq(String tenantId, String commandId, Throwable failure, long nowEpochMs);
}
