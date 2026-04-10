package org.pipelineframework.checkpoint;

import java.util.List;

/**
 * Describes the stable checkpoint publication boundary for the current pipeline.
 */
public interface CheckpointPublicationDescriptor {

    /**
     * Compile-time logical publication name for the final checkpoint output.
     *
     * @return publication name
     */
    String publication();

    /**
     * Stable payload fields used to derive a handoff idempotency key when no stable execution key is available.
     *
     * @return ordered field names
     */
    List<String> idempotencyKeyFields();

    /**
     * Normalize the final pipeline result into the stable checkpoint contract payload.
     *
     * <p>Implementations may override this when the final runtime result needs to be converted into
     * the checkpoint contract that should be published. Returning {@code null} skips publication.</p>
     *
     * @param resultPayload raw pipeline result payload
     * @return normalized checkpoint payload, or {@code null} to skip publication
     */
    default Object normalizePayload(Object resultPayload) {
        return resultPayload;
    }
}
