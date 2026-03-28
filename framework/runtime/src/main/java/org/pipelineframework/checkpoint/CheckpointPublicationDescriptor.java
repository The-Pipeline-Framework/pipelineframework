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
     * <p>This allows runtimes such as {@code pipeline-runtime} to publish the declared pipeline-domain output even
     * when the in-process execution result is still a transport-specific DTO.
     *
     * @param resultPayload raw execution result payload
     * @return normalized checkpoint payload
     */
    default Object normalizePayload(Object resultPayload) {
        return resultPayload;
    }
}
