package org.pipelineframework.checkpoint;

import io.smallrye.mutiny.Uni;

/**
 * Dispatches one published checkpoint to a concrete configured target.
 */
public interface CheckpointPublicationTargetDispatcher {

    /**
     * Target kind supported by this dispatcher.
     *
     * @return target kind
     */
    PublicationTargetKind kind();

    /**
     * Dispatch one published checkpoint to the resolved target.
     *
     * @param target resolved target configuration
     * @param request checkpoint publication request
     * @param tenantId tenant identifier
     * @param idempotencyKey stable handoff idempotency key
     * @return completion signal for downstream admission
     */
    Uni<Void> dispatch(
        ResolvedCheckpointPublicationTarget target,
        CheckpointPublicationRequest request,
        String tenantId,
        String idempotencyKey
    );
}
