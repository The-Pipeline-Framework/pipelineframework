package org.pipelineframework.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.pipelineframework.checkpoint.grpc.CheckpointPublishAcceptedResponse;
import org.pipelineframework.checkpoint.grpc.CheckpointPublishRequest;
import org.pipelineframework.checkpoint.grpc.MutinyCheckpointPublicationServiceGrpc;
import org.pipelineframework.config.pipeline.PipelineJson;

class GrpcCheckpointPublicationTargetDispatcherTest {

    @Test
    void resolveTargetBuildsEndpointFromHostAndPort() {
        GrpcCheckpointPublicationTargetDispatcher dispatcher = new GrpcCheckpointPublicationTargetDispatcher();
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.host()).thenReturn(Optional.of("downstream-host"));
        when(config.port()).thenReturn(Optional.of(9090));
        when(config.plaintext()).thenReturn(false);

        ResolvedCheckpointPublicationTarget target = dispatcher.resolveTarget("orders-ready", "deliver", config);

        assertEquals("orders-ready", target.publication());
        assertEquals("deliver", target.targetId());
        assertEquals(PublicationTargetKind.GRPC, target.kind());
        assertEquals(PublicationEncoding.PROTO, target.encoding());
        assertEquals("downstream-host:9090", target.endpoint());
        assertEquals("TLS", target.method());
    }

    @Test
    void resolveTargetUsesPlaintextWhenConfigured() {
        GrpcCheckpointPublicationTargetDispatcher dispatcher = new GrpcCheckpointPublicationTargetDispatcher();
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.host()).thenReturn(Optional.of("localhost"));
        when(config.port()).thenReturn(Optional.of(9000));
        when(config.plaintext()).thenReturn(true);

        ResolvedCheckpointPublicationTarget target = dispatcher.resolveTarget("orders-ready", "deliver", config);

        assertEquals("PLAINTEXT", target.method());
    }

    @Test
    void resolveTargetFailsWhenHostAbsent() {
        GrpcCheckpointPublicationTargetDispatcher dispatcher = new GrpcCheckpointPublicationTargetDispatcher();
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.host()).thenReturn(Optional.empty());
        when(config.port()).thenReturn(Optional.of(9000));

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> dispatcher.resolveTarget("orders-ready", "deliver", config));
        assertEquals(
            "Checkpoint publication 'orders-ready' target 'deliver' requires host for GRPC delivery",
            error.getMessage());
    }

    @Test
    void resolveTargetFailsWhenHostBlank() {
        GrpcCheckpointPublicationTargetDispatcher dispatcher = new GrpcCheckpointPublicationTargetDispatcher();
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.host()).thenReturn(Optional.of("   "));
        when(config.port()).thenReturn(Optional.of(9000));

        assertThrows(IllegalStateException.class,
            () -> dispatcher.resolveTarget("orders-ready", "deliver", config));
    }

    @Test
    void resolveTargetFailsWhenPortAbsent() {
        GrpcCheckpointPublicationTargetDispatcher dispatcher = new GrpcCheckpointPublicationTargetDispatcher();
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.host()).thenReturn(Optional.of("localhost"));
        when(config.port()).thenReturn(Optional.empty());

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> dispatcher.resolveTarget("orders-ready", "deliver", config));
        assertEquals(
            "Checkpoint publication 'orders-ready' target 'deliver' requires port for GRPC delivery",
            error.getMessage());
    }

    @Test
    void resolveTargetFailsWhenPortIsZero() {
        GrpcCheckpointPublicationTargetDispatcher dispatcher = new GrpcCheckpointPublicationTargetDispatcher();
        PipelineHandoffConfig.TargetConfig config = mock(PipelineHandoffConfig.TargetConfig.class);
        when(config.host()).thenReturn(Optional.of("localhost"));
        when(config.port()).thenReturn(Optional.of(0));

        assertThrows(IllegalStateException.class,
            () -> dispatcher.resolveTarget("orders-ready", "deliver", config));
    }

    @Test
    void resolveTargetKindIsGrpc() {
        GrpcCheckpointPublicationTargetDispatcher dispatcher = new GrpcCheckpointPublicationTargetDispatcher();
        assertEquals(PublicationTargetKind.GRPC, dispatcher.kind());
    }

    @Test
    void dispatchSendsCanonicalProtoRequest() throws Exception {
        AtomicReference<CheckpointPublishRequest> captured = new AtomicReference<>();
        GrpcCheckpointPublicationTargetDispatcher dispatcher = new GrpcCheckpointPublicationTargetDispatcher();
        Server server = ServerBuilder.forPort(0)
            .addService(new TestService(captured))
            .build()
            .start();
        try {
            int port = server.getPort();
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
                "idem-1").await().atMost(Duration.ofSeconds(5));

            CheckpointPublishRequest request = captured.get();
            assertEquals("orders-ready", request.getPublication());
            assertEquals("tenant-1", request.getTenantId());
            assertEquals("idem-1", request.getIdempotencyKey());
            assertEquals("o-1", PipelineJson.mapper().readTree(request.getPayloadJson().toStringUtf8()).get("orderId").asText());
        } finally {
            dispatcher.shutdown();
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
