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
     * Resolve one configured target for this dispatcher kind.
     *
     * @param publication logical publication name
     * @param targetId configured target id
     * @param target target configuration
     * @return resolved target configuration
     */
    ResolvedCheckpointPublicationTarget resolveTarget(
        String publication,
        String targetId,
        PipelineHandoffConfig.TargetConfig target
    );

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
