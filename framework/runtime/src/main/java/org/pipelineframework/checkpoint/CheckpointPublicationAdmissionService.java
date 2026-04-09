package org.pipelineframework.checkpoint;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.NotFoundException;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

/**
 * Shared transport-agnostic admission path for checkpoint publications.
 */
@ApplicationScoped
public class CheckpointPublicationAdmissionService {

    @Inject
    Instance<CheckpointSubscriptionHandler> subscriptionHandlers;

    public Uni<RunAsyncAcceptedDto> admit(
        CheckpointPublicationRequest request,
        String tenantId,
        String idempotencyKey
    ) {
        if (request == null) {
            throw new IllegalArgumentException("Checkpoint publication request must not be null");
        }
        String publication = normalize(request.publication());
        if (publication == null) {
            throw new IllegalArgumentException("Checkpoint publication request must include a non-blank publication");
        }
        JsonNode payload = request.payload();
        if (payload == null || payload.isNull()) {
            throw new IllegalArgumentException("Checkpoint publication request must include a payload");
        }
        CheckpointSubscriptionHandler handler = subscriptionHandlers.stream()
            .filter(candidate -> publication.equals(candidate.publication()))
            .findFirst()
            .orElseThrow(() -> new NotFoundException(
                "No checkpoint subscription handler is configured for publication '" + publication + "'"));
        return handler.admit(payload, normalize(tenantId), normalize(idempotencyKey));
    }

    private String normalize(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
