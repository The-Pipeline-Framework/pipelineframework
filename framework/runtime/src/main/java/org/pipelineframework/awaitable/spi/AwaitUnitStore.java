package org.pipelineframework.awaitable.spi;

import java.util.Optional;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitUnitCreateCommand;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;

/**
 * Persistence SPI for await orchestration units.
 */
public interface AwaitUnitStore {

    default String providerName() {
        return "memory";
    }

    default int priority() {
        return 0;
    }

    Uni<AwaitUnitRecord> createOrGet(AwaitUnitCreateCommand command);

    Uni<Optional<AwaitUnitRecord>> get(String tenantId, String unitId);

    /**
     * Imports a unit created by a transition worker. Existing records win so the operation is idempotent.
     */
    Uni<AwaitUnitRecord> importRecord(AwaitUnitRecord record);

    Uni<Optional<AwaitUnitRecord>> attachPrimaryInteraction(
        String tenantId,
        String unitId,
        String interactionId,
        long nowEpochMs);

    Uni<Optional<AwaitUnitRecord>> markDispatchComplete(
        String tenantId,
        String unitId,
        int expectedItemCount,
        long nowEpochMs);

    Uni<Optional<AwaitUnitRecord>> recordItemCompleted(
        String tenantId,
        String unitId,
        String itemCompletionKey,
        long nowEpochMs);

    Uni<Optional<AwaitUnitRecord>> markCompleted(
        String tenantId,
        String unitId,
        long nowEpochMs);

    Uni<Optional<AwaitUnitRecord>> markTerminal(
        String tenantId,
        String unitId,
        AwaitUnitStatus status,
        long nowEpochMs);
}
