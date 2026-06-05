package org.pipelineframework.orchestrator;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.grpc.TransitionWorkerCapabilitiesRequest;
import org.pipelineframework.orchestrator.grpc.TransitionWorkerCapabilitiesResponse;
import org.pipelineframework.orchestrator.grpc.TransitionWorkerRequest;
import org.pipelineframework.orchestrator.grpc.TransitionWorkerResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GrpcTransitionWorkerServiceTest {

    private final JsonTransitionPayloadCodec payloadCodec = new JsonTransitionPayloadCodec();
    private GrpcTransitionWorkerService service;
    private PipelineOrchestratorConfig config;
    private PipelineOrchestratorConfig.GrpcWorkerConfig grpcConfig;
    private PipelineExecutionService executionService;
    private PipelineBundleIdentityResolver identityResolver;

    @BeforeEach
    void setUp() {
        service = new GrpcTransitionWorkerService();
        config = mock(PipelineOrchestratorConfig.class);
        grpcConfig = mock(PipelineOrchestratorConfig.GrpcWorkerConfig.class);
        executionService = mock(PipelineExecutionService.class);
        identityResolver = mock(PipelineBundleIdentityResolver.class);
        service.orchestratorConfig = config;
        service.executionService = executionService;
        service.identityResolver = identityResolver;
        when(config.workerGrpc()).thenReturn(grpcConfig);
        when(grpcConfig.sharedSecret()).thenReturn(Optional.of("worker-secret"));
        when(grpcConfig.sharedSecretRef()).thenReturn(Optional.empty());
        when(grpcConfig.signatureTolerance()).thenReturn(Duration.ofMinutes(2));
        when(identityResolver.pipelineId(config)).thenReturn("org.example.restaurant");
        when(identityResolver.bundleVersionId(config)).thenReturn("sha256:bundle");
        when(identityResolver.bundleHash()).thenReturn("bundle");
        when(identityResolver.capabilities()).thenReturn(PipelineBundleCapabilities.defaults());
    }

    @Test
    void rejectsCallsWhenServerDisabled() throws Exception {
        when(grpcConfig.serverEnabled()).thenReturn(false);

        StatusRuntimeException error = assertThrows(
            StatusRuntimeException.class,
            () -> service.execute(signed(envelope())).await().indefinitely());

        assertEquals(Status.Code.UNIMPLEMENTED, error.getStatus().getCode());
    }

    @Test
    void delegatesToLocalExecutionServiceWhenEnabled() throws Exception {
        TransitionCommandEnvelope envelope = envelope();
        TransitionResultEnvelope result = TransitionResultEnvelope.completed(payloadCodec, List.of("ok"));
        when(grpcConfig.serverEnabled()).thenReturn(true);
        when(executionService.executeTransition(envelope))
            .thenReturn(Uni.createFrom().item(result));

        TransitionWorkerResponse response = service.execute(signed(envelope)).await().indefinitely();
        TransitionResultEnvelope decoded = PipelineJson.mapper()
            .readValue(response.getResultEnvelope().toByteArray(), TransitionResultEnvelope.class);

        assertEquals(TransitionWorkerOutcome.COMPLETED, decoded.outcome());
        assertEquals(List.of("ok"), decoded.decodeOutputItems(payloadCodec));
    }

    @Test
    void delegatesWhenServerUsesSharedSecretReference() throws Exception {
        TransitionCommandEnvelope envelope = envelope();
        TransitionResultEnvelope result = TransitionResultEnvelope.completed(payloadCodec, List.of("ok"));
        when(grpcConfig.serverEnabled()).thenReturn(true);
        when(grpcConfig.sharedSecret()).thenReturn(Optional.empty());
        when(grpcConfig.sharedSecretRef()).thenReturn(Optional.of("sys:tpf.grpc.worker.secret"));
        System.setProperty("tpf.grpc.worker.secret", "worker-secret");
        when(executionService.executeTransition(envelope))
            .thenReturn(Uni.createFrom().item(result));

        try {
            TransitionWorkerResponse response = service.execute(signed(envelope)).await().indefinitely();
            TransitionResultEnvelope decoded = PipelineJson.mapper()
                .readValue(response.getResultEnvelope().toByteArray(), TransitionResultEnvelope.class);

            assertEquals(TransitionWorkerOutcome.COMPLETED, decoded.outcome());
        } finally {
            System.clearProperty("tpf.grpc.worker.secret");
        }
    }

    @Test
    void rejectsUnsignedRequestWhenServerEnabled() {
        when(grpcConfig.serverEnabled()).thenReturn(true);

        StatusRuntimeException error = assertThrows(
            StatusRuntimeException.class,
            () -> service.execute(TransitionWorkerRequest.newBuilder()
                .setProtocolVersion(GrpcTransitionWorkerProtocol.PROTOCOL_VERSION)
                .setPayloadEncoding(GrpcTransitionWorkerProtocol.PAYLOAD_ENCODING)
                .build()).await().indefinitely());

        assertEquals(Status.Code.UNAUTHENTICATED, error.getStatus().getCode());
    }

    @Test
    void returnsSignedWorkerCapabilitiesWhenEnabled() throws Exception {
        when(grpcConfig.serverEnabled()).thenReturn(true);

        TransitionWorkerCapabilitiesResponse response = service.capabilities(signedCapabilities()).await().indefinitely();
        PipelineWorkerCapability capability = PipelineJson.mapper()
            .readValue(response.getCapabilityEnvelope().toByteArray(), PipelineWorkerCapability.class);

        assertEquals("grpc", capability.providerName());
        assertEquals("org.example.restaurant", capability.pipelineId());
        assertEquals("sha256:bundle", capability.bundleVersionId());
    }

    @Test
    void rejectsMissingServerSecret() throws Exception {
        when(grpcConfig.serverEnabled()).thenReturn(true);
        when(grpcConfig.sharedSecret()).thenReturn(Optional.empty());
        when(grpcConfig.sharedSecretRef()).thenReturn(Optional.empty());

        StatusRuntimeException error = assertThrows(
            StatusRuntimeException.class,
            () -> service.execute(signed(envelope())).await().indefinitely());

        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
    }

    @Test
    void rejectsReplayedNonce() throws Exception {
        TransitionCommandEnvelope envelope = envelope();
        TransitionResultEnvelope result = TransitionResultEnvelope.completed(payloadCodec, List.of("ok"));
        when(grpcConfig.serverEnabled()).thenReturn(true);
        when(executionService.executeTransition(envelope))
            .thenReturn(Uni.createFrom().item(result));
        TransitionWorkerRequest request = signed(envelope);

        service.execute(request).await().indefinitely();
        StatusRuntimeException replay = assertThrows(
            StatusRuntimeException.class,
            () -> service.execute(request).await().indefinitely());

        assertEquals(Status.Code.UNAUTHENTICATED, replay.getStatus().getCode());
    }

    @Test
    void rejectsMalformedPayload() throws Exception {
        when(grpcConfig.serverEnabled()).thenReturn(true);
        TransitionWorkerRequest request = signed("{not-json".getBytes(StandardCharsets.UTF_8));

        StatusRuntimeException error = assertThrows(
            StatusRuntimeException.class,
            () -> service.execute(request).await().indefinitely());

        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
    }

    private TransitionCommandEnvelope envelope() {
        return TransitionEnvelopeFixtures.envelope(payloadCodec);
    }

    private TransitionWorkerRequest signed(TransitionCommandEnvelope envelope) throws Exception {
        return signed(PipelineJson.mapper().writeValueAsBytes(envelope));
    }

    private TransitionWorkerRequest signed(byte[] body) {
        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(
            "worker-secret",
            GrpcTransitionWorkerProtocol.SIGNATURE_METHOD,
            GrpcTransitionWorkerProtocol.SIGNATURE_PATH,
            timestamp,
            nonce,
            body);
        return TransitionWorkerRequest.newBuilder()
            .setProtocolVersion(GrpcTransitionWorkerProtocol.PROTOCOL_VERSION)
            .setPayloadEncoding(GrpcTransitionWorkerProtocol.PAYLOAD_ENCODING)
            .setCommandEnvelope(ByteString.copyFrom(body))
            .setTimestamp(timestamp)
            .setNonce(nonce)
            .setSignature(signature)
            .build();
    }

    private TransitionWorkerCapabilitiesRequest signedCapabilities() {
        byte[] body = new byte[0];
        String timestamp = Instant.now().toString();
        String nonce = UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(
            "worker-secret",
            GrpcTransitionWorkerProtocol.SIGNATURE_METHOD,
            GrpcTransitionWorkerProtocol.CAPABILITIES_SIGNATURE_PATH,
            timestamp,
            nonce,
            body);
        return TransitionWorkerCapabilitiesRequest.newBuilder()
            .setProtocolVersion(GrpcTransitionWorkerProtocol.PROTOCOL_VERSION)
            .setTimestamp(timestamp)
            .setNonce(nonce)
            .setSignature(signature)
            .build();
    }
}
