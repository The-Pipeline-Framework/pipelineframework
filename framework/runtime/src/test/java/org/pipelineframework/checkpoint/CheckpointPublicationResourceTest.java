package org.pipelineframework.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import io.smallrye.mutiny.Uni;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.pipelineframework.checkpoint.grpc.CheckpointPublishRequest;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

class CheckpointPublicationResourceTest {

    @Test
    void publishProtobufDecodesRequestAndRoutesByPublication() throws Exception {
        CheckpointPublicationResource resource = new CheckpointPublicationResource();
        CheckpointPublicationAdmissionService admissionService = new CheckpointPublicationAdmissionService();
        TestHandler handler = new TestHandler();
        admissionService.subscriptionHandlers = handlers(handler);
        resource.admissionService = admissionService;

        byte[] body = CheckpointPublishRequest.newBuilder()
            .setPublication("orders-ready")
            .setPayloadJson(com.google.protobuf.ByteString.copyFromUtf8(
                PipelineJson.mapper().writeValueAsString(PipelineJson.mapper().valueToTree(
                    new PublishedOrder("o-1", "c-1")))))
            .setTenantId("tenant-1")
            .setIdempotencyKey("idem-1")
            .build()
            .toByteArray();

        RunAsyncAcceptedDto response = resource.publishProtobuf(body, null, null).await().indefinitely();

        assertEquals("exec-1", response.executionId());
        assertEquals("orders-ready", handler.lastPublication);
        assertEquals("tenant-1", handler.lastTenantId);
        assertEquals("idem-1", handler.lastIdempotencyKey);
        assertEquals("o-1", handler.lastPayload.get("orderId").asText());
    }

    @SuppressWarnings("unchecked")
    private Instance<CheckpointSubscriptionHandler> handlers(CheckpointSubscriptionHandler... handlers) {
        Instance<CheckpointSubscriptionHandler> instance = mock(Instance.class);
        when(instance.stream()).thenReturn(Stream.of(handlers));
        return instance;
    }

    private record PublishedOrder(String orderId, String customerId) {
    }

    private static final class TestHandler implements CheckpointSubscriptionHandler {
        private String lastPublication;
        private com.fasterxml.jackson.databind.JsonNode lastPayload;
        private String lastTenantId;
        private String lastIdempotencyKey;

        @Override
        public String publication() {
            return "orders-ready";
        }

        @Override
        public Uni<RunAsyncAcceptedDto> admit(
            com.fasterxml.jackson.databind.JsonNode payload,
            String tenantId,
            String idempotencyKey
        ) {
            this.lastPublication = publication();
            this.lastPayload = payload;
            this.lastTenantId = tenantId;
            this.lastIdempotencyKey = idempotencyKey;
            return Uni.createFrom().item(new RunAsyncAcceptedDto("exec-1", false, "/status/exec-1", 1L));
        }
    }
}
