package org.pipelineframework.checkpoint;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

/**
 * Subscriber-side admission handler for one logical checkpoint publication.
 */
public interface CheckpointSubscriptionHandler {

    /**
     * Logical publication name handled by this subscriber.
     *
     * @return publication name
     */
    String publication();

    /**
     * Admit one published checkpoint into downstream async orchestration.
     *
     * @param payload serialized checkpoint payload
     * @param tenantId tenant identifier
     * @param idempotencyKey stable handoff idempotency key
     * @return async-admission response
     */
    Uni<RunAsyncAcceptedDto> admit(JsonNode payload, String tenantId, String idempotencyKey);
}
