package org.pipelineframework.orchestrator;

import org.pipelineframework.orchestrator.worker.PipelineWorkerCapability;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.invocation.TransportBoundaryInvocation;
import org.pipelineframework.orchestrator.grpc.MutinyTransitionWorkerServiceGrpc;
import org.pipelineframework.orchestrator.grpc.TransitionWorkerCapabilitiesRequest;
import org.pipelineframework.orchestrator.grpc.TransitionWorkerCapabilitiesResponse;
import org.pipelineframework.orchestrator.grpc.TransitionWorkerRequest;
import org.pipelineframework.orchestrator.grpc.TransitionWorkerResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GrpcPipelineTransitionWorkerTest {

    private final JsonTransitionPayloadCodec payloadCodec = new JsonTransitionPayloadCodec();
    private Server server;
    private GrpcPipelineTransitionWorker worker;
    private PipelineOrchestratorConfig config;
    private PipelineOrchestratorConfig.GrpcWorkerConfig grpcConfig;

    @BeforeEach
    void setUp() {
        config = mock(PipelineOrchestratorConfig.class);
        grpcConfig = mock(PipelineOrchestratorConfig.GrpcWorkerConfig.class);
        when(config.workerGrpc()).thenReturn(grpcConfig);
        when(grpcConfig.plaintext()).thenReturn(true);
        when(grpcConfig.requestTimeout()).thenReturn(Duration.ofSeconds(5));
        when(grpcConfig.sharedSecret()).thenReturn(Optional.of("worker-secret"));
        when(grpcConfig.sharedSecretRef()).thenReturn(Optional.empty());
    }

    @AfterEach
    void tearDown() {
        if (worker != null) {
            worker.closeChannel();
        }
        if (server != null) {
            server.shutdownNow();
        }
    }

    @Test
    void sendsSignedCommandEnvelopeAndDecodesCompletedResult() throws Exception {
        AtomicReference<TransitionWorkerRequest> captured = new AtomicReference<>();
        server = ServerBuilder.forPort(0)
            .addService(new TestService(request -> {
                captured.set(request);
                return response(TransitionResultEnvelope.completed(payloadCodec, List.of("ok")));
            }))
            .build()
            .start();
        when(grpcConfig.endpoint()).thenReturn(Optional.of("localhost:" + server.getPort()));
        worker = newWorker();
        worker.orchestratorConfig = config;

        TransitionResultEnvelope result = worker.executeTransition(envelope()).await().indefinitely();

        assertEquals(TransitionWorkerOutcome.COMPLETED, result.outcome());
        assertEquals(List.of("ok"), result.decodeOutputItems(payloadCodec));
        assertEquals(GrpcTransitionWorkerProtocol.PROTOCOL_VERSION, captured.get().getProtocolVersion());
        assertEquals(GrpcTransitionWorkerProtocol.PAYLOAD_ENCODING, captured.get().getPayloadEncoding());
        assertTrue(captured.get().getSignature() != null && !captured.get().getSignature().isBlank());
        assertTrue(captured.get().getCommandEnvelope().toStringUtf8().contains("\"transitionKey\":\"exec-1:0:0\""));
    }

    @Test
    void exposesTransportBoundaryDiagnostics() {
        worker = newWorker();

        assertTrue(worker instanceof TransportBoundaryInvocation);
        assertEquals("grpc", worker.transportBoundary().protocol());
        assertEquals("transition-worker.execute", worker.transportBoundary().target());
    }

    @Test
    void sendsSignedCommandWithSharedSecretReference() throws Exception {
        AtomicReference<TransitionWorkerRequest> captured = new AtomicReference<>();
        when(grpcConfig.sharedSecret()).thenReturn(Optional.empty());
        when(grpcConfig.sharedSecretRef()).thenReturn(Optional.of("sys:tpf.grpc.worker.secret"));
        System.setProperty("tpf.grpc.worker.secret", "worker-secret");
        server = ServerBuilder.forPort(0)
            .addService(new TestService(request -> {
                captured.set(request);
                return response(TransitionResultEnvelope.completed(payloadCodec, List.of("ok")));
            }))
            .build()
            .start();
        when(grpcConfig.endpoint()).thenReturn(Optional.of("localhost:" + server.getPort()));
        worker = newWorker();
        worker.orchestratorConfig = config;

        try {
            TransitionResultEnvelope result = worker.executeTransition(envelope()).await().indefinitely();

            assertEquals(TransitionWorkerOutcome.COMPLETED, result.outcome());
            assertTrue(captured.get().getSignature() != null && !captured.get().getSignature().isBlank());
        } finally {
            System.clearProperty("tpf.grpc.worker.secret");
        }
    }

    @Test
    void fetchesSignedWorkerCapabilities() throws Exception {
        AtomicReference<TransitionWorkerCapabilitiesRequest> captured = new AtomicReference<>();
        server = ServerBuilder.forPort(0)
            .addService(new TestService(
                request -> response(TransitionResultEnvelope.completed(payloadCodec, List.of("ok"))),
                request -> {
                    captured.set(request);
                    return capabilitiesResponse();
                }))
            .build()
            .start();
        when(grpcConfig.endpoint()).thenReturn(Optional.of("localhost:" + server.getPort()));
        worker = newWorker();
        worker.orchestratorConfig = config;

        PipelineWorkerCapability capability = worker.capabilities().await().indefinitely();

        assertEquals("grpc", capability.providerName());
        assertEquals("org.example.restaurant", capability.pipelineId());
        assertEquals("sha256:contract", capability.contractVersion());
        assertEquals("sha256:release", capability.releaseVersion());
        assertTrue(captured.get().getSignature() != null && !captured.get().getSignature().isBlank());
    }

    @Test
    void failsOnMalformedResultEnvelope() throws Exception {
        server = ServerBuilder.forPort(0)
            .addService(new TestService(request -> TransitionWorkerResponse.newBuilder()
                .setProtocolVersion(GrpcTransitionWorkerProtocol.PROTOCOL_VERSION)
                .setPayloadEncoding(GrpcTransitionWorkerProtocol.PAYLOAD_ENCODING)
                .setResultEnvelope(ByteString.copyFromUtf8("{not-json"))
                .build()))
            .build()
            .start();
        when(grpcConfig.endpoint()).thenReturn(Optional.of("localhost:" + server.getPort()));
        worker = newWorker();
        worker.orchestratorConfig = config;

        TransitionWorkerFailureException error = assertThrows(
            TransitionWorkerFailureException.class,
            () -> worker.executeTransition(envelope()).await().indefinitely());

        assertTrue(error.getMessage().contains("malformed JSON"));
    }

    @Test
    void requiresSharedSecretAtStartupValidation() {
        worker = newWorker();
        when(grpcConfig.isEnabled()).thenReturn(true);
        when(grpcConfig.sharedSecret()).thenReturn(Optional.empty());
        when(grpcConfig.sharedSecretRef()).thenReturn(Optional.empty());

        Optional<String> error = worker.startupValidationError(config);

        assertTrue(error.isPresent());
        assertTrue(error.get().contains("pipeline.orchestrator.worker.grpc.shared-secret"));
    }

    @Test
    void acceptsSharedSecretReferenceAtStartupValidation() {
        worker = newWorker();
        when(grpcConfig.isEnabled()).thenReturn(true);
        when(grpcConfig.sharedSecret()).thenReturn(Optional.empty());
        when(grpcConfig.sharedSecretRef()).thenReturn(Optional.of("env:TPF_GRPC_WORKER_SECRET"));

        Optional<String> error = worker.startupValidationError(config);

        assertTrue(error.isEmpty());
    }

    @Test
    void rejectsAmbiguousSharedSecretAtStartupValidation() {
        worker = newWorker();
        when(grpcConfig.isEnabled()).thenReturn(true);
        when(grpcConfig.sharedSecret()).thenReturn(Optional.of("worker-secret"));
        when(grpcConfig.sharedSecretRef()).thenReturn(Optional.of("env:TPF_GRPC_WORKER_SECRET"));

        Optional<String> error = worker.startupValidationError(config);

        assertTrue(error.isPresent());
        assertTrue(error.get().contains("Configure only one"));
    }

    @Test
    void rejectsBlankEndpointAtStartupValidation() {
        worker = newWorker();
        when(grpcConfig.isEnabled()).thenReturn(true);
        when(grpcConfig.sharedSecret()).thenReturn(Optional.of("worker-secret"));
        when(grpcConfig.endpoint()).thenReturn(Optional.of(" "));

        Optional<String> error = worker.startupValidationError(config);

        assertTrue(error.isPresent());
        assertTrue(error.get().contains("pipeline.orchestrator.worker.grpc.endpoint"));
    }

    private TransitionCommandEnvelope envelope() {
        return TransitionEnvelopeFixtures.envelope(payloadCodec);
    }

    private GrpcPipelineTransitionWorker newWorker() {
        return new GrpcPipelineTransitionWorker(new PipelineInvocationRuntime());
    }

    private TransitionWorkerResponse response(TransitionResultEnvelope result) {
        try {
            return TransitionWorkerResponse.newBuilder()
                .setProtocolVersion(GrpcTransitionWorkerProtocol.PROTOCOL_VERSION)
                .setPayloadEncoding(GrpcTransitionWorkerProtocol.PAYLOAD_ENCODING)
                .setResultEnvelope(ByteString.copyFrom(PipelineJson.mapper().writeValueAsBytes(result)))
                .build();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private TransitionWorkerCapabilitiesResponse capabilitiesResponse() {
        try {
            PipelineWorkerCapability capability = new PipelineWorkerCapability(
                PipelineWorkerCapability.PROTOCOL_VERSION,
                "grpc",
                "org.example.restaurant",
                "sha256:contract",
                "sha256:release",
                "restaurant-approval-monolith",
                "sha256:artifact",
                List.of("application/tpf-transition-envelope+json"),
                List.of("grpc"));
            return TransitionWorkerCapabilitiesResponse.newBuilder()
                .setProtocolVersion(GrpcTransitionWorkerProtocol.PROTOCOL_VERSION)
                .setPayloadEncoding(GrpcTransitionWorkerProtocol.PAYLOAD_ENCODING)
                .setCapabilityEnvelope(ByteString.copyFrom(PipelineJson.mapper().writeValueAsBytes(capability)))
                .build();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static final class TestService
        extends MutinyTransitionWorkerServiceGrpc.TransitionWorkerServiceImplBase {

        private final java.util.function.Function<TransitionWorkerRequest, TransitionWorkerResponse> handler;
        private final java.util.function.Function<TransitionWorkerCapabilitiesRequest, TransitionWorkerCapabilitiesResponse>
            capabilitiesHandler;

        private TestService(java.util.function.Function<TransitionWorkerRequest, TransitionWorkerResponse> handler) {
            this(handler, request -> TransitionWorkerCapabilitiesResponse.newBuilder()
                .setProtocolVersion(GrpcTransitionWorkerProtocol.PROTOCOL_VERSION)
                .setPayloadEncoding(GrpcTransitionWorkerProtocol.PAYLOAD_ENCODING)
                .build());
        }

        private TestService(
            java.util.function.Function<TransitionWorkerRequest, TransitionWorkerResponse> handler,
            java.util.function.Function<TransitionWorkerCapabilitiesRequest, TransitionWorkerCapabilitiesResponse>
                capabilitiesHandler) {
            this.handler = handler;
            this.capabilitiesHandler = capabilitiesHandler;
        }

        @Override
        public Uni<TransitionWorkerResponse> execute(TransitionWorkerRequest request) {
            return Uni.createFrom().item(() -> handler.apply(request));
        }

        @Override
        public Uni<TransitionWorkerCapabilitiesResponse> capabilities(TransitionWorkerCapabilitiesRequest request) {
            return Uni.createFrom().item(() -> capabilitiesHandler.apply(request));
        }
    }
}
