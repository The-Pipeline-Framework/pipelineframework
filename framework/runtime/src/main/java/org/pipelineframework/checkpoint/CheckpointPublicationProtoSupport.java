package org.pipelineframework.checkpoint;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.checkpoint.grpc.CheckpointPublishAcceptedResponse;
import org.pipelineframework.checkpoint.grpc.CheckpointPublishRequest;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

/**
 * Shared helpers for the framework-owned checkpoint protobuf contract.
 */
public final class CheckpointPublicationProtoSupport {

    private static final ObjectMapper JSON = PipelineJson.mapper();

    private CheckpointPublicationProtoSupport() {
    }

    public static CheckpointPublishRequest toProtoRequest(
        CheckpointPublicationRequest request,
        String tenantId,
        String idempotencyKey
    ) throws IOException {
        return CheckpointPublishRequest.newBuilder()
            .setPublication(request.publication() == null ? "" : request.publication())
            .setPayloadJson(com.google.protobuf.ByteString.copyFrom(
                JSON.writeValueAsBytes(request.payload() == null ? JSON.nullNode() : request.payload())))
            .setTenantId(tenantId == null ? "" : tenantId)
            .setIdempotencyKey(idempotencyKey == null ? "" : idempotencyKey)
            .build();
    }

    public static CheckpointPublicationRequest fromProtoRequest(CheckpointPublishRequest request) throws IOException {
        if (request == null) {
            throw new IllegalArgumentException("Checkpoint protobuf request must not be null");
        }
        byte[] payloadBytes = request.getPayloadJson().toByteArray();
        if (payloadBytes.length == 0) {
            throw new IllegalArgumentException("Checkpoint protobuf request must include payload_json");
        }
        JsonNode payload = JSON.readTree(new String(payloadBytes, StandardCharsets.UTF_8));
        return new CheckpointPublicationRequest(request.getPublication(), payload);
    }

    public static CheckpointPublishAcceptedResponse toProtoResponse(RunAsyncAcceptedDto accepted) {
        return CheckpointPublishAcceptedResponse.newBuilder()
            .setExecutionId(accepted.executionId() == null ? "" : accepted.executionId())
            .setDuplicate(accepted.duplicate())
            .setStatusUrl(accepted.statusUrl() == null ? "" : accepted.statusUrl())
            .setSubmittedAtEpochMs(accepted.submittedAtEpochMs())
            .build();
    }
}
