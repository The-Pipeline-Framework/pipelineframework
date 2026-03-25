package org.pipelineframework.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.pipelineframework.checkpoint.grpc.CheckpointPublishAcceptedResponse;
import org.pipelineframework.checkpoint.grpc.CheckpointPublishRequest;
import org.pipelineframework.checkpoint.grpc.MutinyCheckpointPublicationServiceGrpc;
import org.pipelineframework.config.pipeline.PipelineJson;

class GrpcCheckpointPublicationTargetDispatcherTest {

    @Test
    void dispatchSendsCanonicalProtoRequest() throws Exception {
        AtomicReference<CheckpointPublishRequest> captured = new AtomicReference<>();
        Server server = ServerBuilder.forPort(0)
            .addService(new TestService(captured))
            .build()
            .start();
        try {
            int port = server.getPort();
            GrpcCheckpointPublicationTargetDispatcher dispatcher = new GrpcCheckpointPublicationTargetDispatcher();
            ResolvedCheckpointPublicationTarget target = new ResolvedCheckpointPublicationTarget(
                "orders-ready",
                "deliver",
                PublicationTargetKind.GRPC,
                PublicationEncoding.PROTO,
                null,
                null,
                "localhost:" + port,
                "PLAINTEXT");

            dispatcher.dispatch(
                target,
                new CheckpointPublicationRequest(
                    "orders-ready",
                    PipelineJson.mapper().valueToTree(new PublishedOrder("o-1"))),
                "tenant-1",
                "idem-1").await().indefinitely();

            CheckpointPublishRequest request = captured.get();
            assertEquals("orders-ready", request.getPublication());
            assertEquals("tenant-1", request.getTenantId());
            assertEquals("idem-1", request.getIdempotencyKey());
            assertEquals("o-1", PipelineJson.mapper().readTree(request.getPayloadJson().toStringUtf8()).get("orderId").asText());
            dispatcher.shutdown();
        } finally {
            server.shutdownNow();
        }
    }

    private record PublishedOrder(String orderId) {
    }

    private static final class TestService extends MutinyCheckpointPublicationServiceGrpc.CheckpointPublicationServiceImplBase {
        private final AtomicReference<CheckpointPublishRequest> captured;

        private TestService(AtomicReference<CheckpointPublishRequest> captured) {
            this.captured = captured;
        }

        @Override
        public io.smallrye.mutiny.Uni<CheckpointPublishAcceptedResponse> publish(CheckpointPublishRequest request) {
            captured.set(request);
            return io.smallrye.mutiny.Uni.createFrom().item(
                CheckpointPublishAcceptedResponse.newBuilder()
                    .setExecutionId("exec-1")
                    .setStatusUrl("/status/exec-1")
                    .setSubmittedAtEpochMs(1L)
                    .build());
        }
    }
}
