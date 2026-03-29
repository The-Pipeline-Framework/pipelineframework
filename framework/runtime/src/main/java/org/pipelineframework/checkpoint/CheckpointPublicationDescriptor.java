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
 * Defines the ordered payload field names used to derive a handoff idempotency key when no stable execution key is available.
 *
 * @return the ordered list of stable payload field names used to derive the idempotency key
 */
    List<String> idempotencyKeyFields();

    /**
     * Normalize the final pipeline result into the stable checkpoint contract payload.
     *
     * <p>Runtimes may override this to translate a transport-specific execution result DTO into the
     * pipeline's stable checkpoint payload. Implementations may return {@code null} to indicate that
     * checkpoint publication should be skipped for the given result.
     *
     * @param resultPayload the raw execution result payload to normalize
     * @return the normalized checkpoint payload, or {@code null} to skip publication
     */
    default Object normalizePayload(Object resultPayload) {
        return resultPayload;
    }
}
