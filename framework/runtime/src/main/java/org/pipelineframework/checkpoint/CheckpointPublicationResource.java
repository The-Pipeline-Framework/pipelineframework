package org.pipelineframework.checkpoint;

import java.io.IOException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;
import org.pipelineframework.transport.http.ProtobufHttpContentTypes;

/**
 * Generic HTTP admission endpoint for checkpoint publications.
 */
@ApplicationScoped
@Path(CheckpointPublicationResource.DEFAULT_PATH)
@Produces("application/json")
public class CheckpointPublicationResource {

    public static final String DEFAULT_PATH = "/pipeline/checkpoints/publish";

    @Inject
    CheckpointPublicationAdmissionService admissionService;

    @POST
    @Consumes("application/json")
    public Uni<RunAsyncAcceptedDto> publishJson(
        CheckpointPublicationRequest request,
        @HeaderParam("x-tenant-id") String tenantId,
        @HeaderParam("Idempotency-Key") String idempotencyKey
    ) {
        return handle(request, tenantId, idempotencyKey);
    }

    @POST
    @Consumes(ProtobufHttpContentTypes.APPLICATION_X_PROTOBUF)
    public Uni<RunAsyncAcceptedDto> publishProtobuf(
        byte[] body,
        @HeaderParam("x-tenant-id") String tenantId,
        @HeaderParam("Idempotency-Key") String idempotencyKey
    ) {
        org.pipelineframework.checkpoint.grpc.CheckpointPublishRequest protoRequest;
        try {
            protoRequest = org.pipelineframework.checkpoint.grpc.CheckpointPublishRequest.parseFrom(
                body == null ? new byte[0] : body);
        } catch (IOException e) {
            throw new BadRequestException("Checkpoint protobuf publication body is invalid", e);
        }
        CheckpointPublicationRequest request;
        try {
            request = CheckpointPublicationProtoSupport.fromProtoRequest(protoRequest);
        } catch (IOException e) {
            throw new BadRequestException("Checkpoint protobuf publication body is invalid", e);
        } catch (IllegalArgumentException e) {
            throw new BadRequestException(e.getMessage(), e);
        }
        String resolvedTenantId = headerOrProto(tenantId, protoRequest.getTenantId());
        String resolvedIdempotencyKey = headerOrProto(idempotencyKey, protoRequest.getIdempotencyKey());
        return handle(request, resolvedTenantId, resolvedIdempotencyKey);
    }

    private Uni<RunAsyncAcceptedDto> handle(
        CheckpointPublicationRequest request,
        String tenantId,
        String idempotencyKey
    ) {
        try {
            return admissionService.admit(request, tenantId, idempotencyKey);
        } catch (IllegalArgumentException e) {
            throw new BadRequestException(e.getMessage(), e);
        }
    }

    private String headerOrProto(String headerValue, String protoValue) {
        String normalizedHeader = normalize(headerValue);
        return normalizedHeader != null ? normalizedHeader : normalize(protoValue);
    }

    private String normalize(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
