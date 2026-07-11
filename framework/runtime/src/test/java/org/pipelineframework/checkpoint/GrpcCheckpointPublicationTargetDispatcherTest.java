package org.pipelineframework.checkpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Context;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.smallrye.mutiny.subscription.Cancellable;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
    void resolveTargetRejectsColonContainingHosts() {
        GrpcCheckpointPublicationTargetDispatcher dispatcher = new GrpcCheckpointPublicationTargetDispatcher();
        PipelineHandoffConfig.TargetConfig target = mock(PipelineHandoffConfig.TargetConfig.class);
        when(target.host()).thenReturn(Optional.of("::1"));
        when(target.port()).thenReturn(Optional.of(9000));

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> dispatcher.resolveTarget("orders-ready", "deliver", target));

        assertEquals(
            "Checkpoint publication 'orders-ready' target 'deliver' does not support colon-containing GRPC hosts; use a DNS name or IPv4 address",
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

    @Test
    void dispatchDoesNotInheritCancelledInboundGrpcContext() throws Exception {
        AtomicReference<CheckpointPublishRequest> captured = new AtomicReference<>();
        GrpcCheckpointPublicationTargetDispatcher dispatcher = new GrpcCheckpointPublicationTargetDispatcher();
        Server server = ServerBuilder.forPort(0)
            .addService(new TestService(captured))
            .build()
            .start();
        Context.CancellableContext inboundContext = Context.current().withCancellation();
        try {
            ResolvedCheckpointPublicationTarget target = new ResolvedCheckpointPublicationTarget(
                "orders-ready",
                "deliver",
                PublicationTargetKind.GRPC,
                PublicationEncoding.PROTO,
                null,
                null,
                "localhost:" + server.getPort(),
                "PLAINTEXT");
            inboundContext.cancel(null);

            inboundContext.run(() -> dispatcher.dispatch(
                target,
                new CheckpointPublicationRequest(
                    "orders-ready",
                    PipelineJson.mapper().valueToTree(new PublishedOrder("o-1"))),
                "tenant-1",
                "idem-1").await().atMost(Duration.ofSeconds(5)));

            assertEquals("orders-ready", captured.get().getPublication());
        } finally {
            inboundContext.close();
            dispatcher.shutdown();
            server.shutdownNow();
        }
    }

    @Test
    void cancellingDispatchCancelsOutboundGrpcCall() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch cancelled = new CountDownLatch(1);
        GrpcCheckpointPublicationTargetDispatcher dispatcher = new GrpcCheckpointPublicationTargetDispatcher();
        Server server = ServerBuilder.forPort(0)
            .addService(ServerInterceptors.intercept(
                new CancellableTestService(started),
                new CancellationInterceptor(cancelled)))
            .build()
            .start();
        try {
            ResolvedCheckpointPublicationTarget target = new ResolvedCheckpointPublicationTarget(
                "orders-ready",
                "deliver",
                PublicationTargetKind.GRPC,
                PublicationEncoding.PROTO,
                null,
                null,
                "localhost:" + server.getPort(),
                "PLAINTEXT");

            Cancellable call = dispatcher.dispatch(
                target,
                new CheckpointPublicationRequest(
                    "orders-ready",
                    PipelineJson.mapper().valueToTree(new PublishedOrder("o-1"))),
                "tenant-1",
                "idem-1").subscribe().with(ignored -> { }, ignored -> { });

            assertTrue(started.await(5, TimeUnit.SECONDS));
            call.cancel();
            assertTrue(cancelled.await(5, TimeUnit.SECONDS));
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

    private static final class CancellableTestService
        extends MutinyCheckpointPublicationServiceGrpc.CheckpointPublicationServiceImplBase {
        private final CountDownLatch started;

        private CancellableTestService(CountDownLatch started) {
            this.started = started;
        }

        @Override
        public io.smallrye.mutiny.Uni<CheckpointPublishAcceptedResponse> publish(CheckpointPublishRequest request) {
            started.countDown();
            return io.smallrye.mutiny.Uni.createFrom().nothing();
        }
    }

    private static final class CancellationInterceptor implements ServerInterceptor {
        private final CountDownLatch cancelled;

        private CancellationInterceptor(CountDownLatch cancelled) {
            this.cancelled = cancelled;
        }

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next
        ) {
            return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(
                next.startCall(call, headers)) {
                @Override
                public void onCancel() {
                    cancelled.countDown();
                    super.onCancel();
                }
            };
        }
    }
}
