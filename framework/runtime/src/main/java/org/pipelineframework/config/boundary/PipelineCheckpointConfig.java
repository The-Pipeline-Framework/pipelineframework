package org.pipelineframework.config.boundary;

import java.util.ArrayList;
import java.util.List;

/**
 * Stable checkpoint publication boundary for a pipeline's final output.
 *
 * @param publication compile-time logical publication name for the checkpoint
 * @param idempotencyKeyFields optional payload fields used to derive a handoff key when none is available
 */
public record PipelineCheckpointConfig(
    String publication,
    List<String> idempotencyKeyFields
) {
    public PipelineCheckpointConfig {
        if (publication == null || publication.isBlank()) {
            throw new IllegalArgumentException("checkpoint publication must not be blank");
        }
        idempotencyKeyFields = idempotencyKeyFields == null
            ? List.of()
            : List.copyOf(new ArrayList<>(idempotencyKeyFields));
    }
}
