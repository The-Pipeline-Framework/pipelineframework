package org.pipelineframework.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.pipelineframework.checkpoint.grpc.CheckpointPublishAcceptedResponse;
import org.pipelineframework.checkpoint.grpc.CheckpointPublishRequest;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

class CheckpointPublicationGrpcServiceTest {

    @Test
    void publishReturnsAcceptedResponse() throws Exception {
        CheckpointPublicationGrpcService service = service(new SuccessHandler());

        CheckpointPublishAcceptedResponse response = service.publish(validRequest()).await().indefinitely();

        assertEquals("exec-1", response.getExecutionId());
        assertEquals("/status/exec-1", response.getStatusUrl());
        assertEquals(1L, response.getSubmittedAtEpochMs());
    }

    @Test
    void publishMapsUnknownPublicationToNotFoundStatus() {
        CheckpointPublicationGrpcService service = service();

        Throwable failure = org.junit.jupiter.api.Assertions.assertThrows(
            Throwable.class,
            () -> service.publish(validRequest()).await().indefinitely());

        StatusRuntimeException status = assertInstanceOf(StatusRuntimeException.class, failure);
        assertEquals(Status.Code.NOT_FOUND, status.getStatus().getCode());
    }

    @Test
    void publishMapsInvalidJsonToInvalidArgument() {
        CheckpointPublicationGrpcService service = service(new SuccessHandler());
        CheckpointPublishRequest request = CheckpointPublishRequest.newBuilder()
            .setPublication("orders-ready")
            .setPayloadJson(com.google.protobuf.ByteString.copyFromUtf8("{not-json"))
            .build();

        Throwable failure = org.junit.jupiter.api.Assertions.assertThrows(
            Throwable.class,
            () -> service.publish(request).await().indefinitely());

        StatusRuntimeException status = assertInstanceOf(StatusRuntimeException.class, failure);
        assertEquals(Status.Code.INVALID_ARGUMENT, status.getStatus().getCode());
    }

    @Test
    void publishMapsHandlerFailureToInternal() {
        CheckpointPublicationGrpcService service = service(new FailingHandler());

        Throwable failure = org.junit.jupiter.api.Assertions.assertThrows(
            Throwable.class,
            () -> service.publish(validRequest()).await().indefinitely());

        StatusRuntimeException status = assertInstanceOf(StatusRuntimeException.class, failure);
        assertEquals(Status.Code.INTERNAL, status.getStatus().getCode());
    }

    private CheckpointPublicationGrpcService service(CheckpointSubscriptionHandler... handlers) {
        CheckpointPublicationAdmissionService admissionService = new CheckpointPublicationAdmissionService();
        admissionService.subscriptionHandlers = handlerInstance(handlers);
        CheckpointPublicationGrpcService service = new CheckpointPublicationGrpcService();
        service.admissionService = admissionService;
        return service;
    }

    @SuppressWarnings("unchecked")
    private Instance<CheckpointSubscriptionHandler> handlerInstance(CheckpointSubscriptionHandler... handlers) {
        Instance<CheckpointSubscriptionHandler> instance = mock(Instance.class);
        when(instance.stream()).thenReturn(Stream.of(handlers));
        return instance;
    }

    private CheckpointPublishRequest validRequest() {
        return CheckpointPublishRequest.newBuilder()
            .setPublication("orders-ready")
            .setPayloadJson(com.google.protobuf.ByteString.copyFromUtf8(
                PipelineJson.mapper().valueToTree(new PublishedOrder("o-1")).toString()))
            .setTenantId("tenant-1")
            .setIdempotencyKey("idem-1")
            .build();
    }

    private record PublishedOrder(String orderId) {
    }

    private static final class SuccessHandler implements CheckpointSubscriptionHandler {
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
            return Uni.createFrom().item(new RunAsyncAcceptedDto("exec-1", false, "/status/exec-1", 1L));
        }
    }

    private static final class FailingHandler implements CheckpointSubscriptionHandler {
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
            return Uni.createFrom().failure(new RuntimeException("boom"));
        }
    }
}
