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
}
