package org.pipelineframework.checkpoint;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Generic checkpoint publication admission request.
 *
 * @param publication logical publication name
 * @param payload serialized checkpoint payload
 */
public record CheckpointPublicationRequest(
    String publication,
    JsonNode payload
) {
    public CheckpointPublicationRequest {
        if (publication == null || publication.isBlank()) {
            throw new IllegalArgumentException("publication must not be blank");
        }
    }
}
