package org.pipelineframework.orchestrator;

import org.pipelineframework.orchestrator.worker.PipelineWorkerCapability;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.config.pipeline.PipelineJson;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RestTransitionWorkerResourceTest {

    private final JsonTransitionPayloadCodec payloadCodec = new JsonTransitionPayloadCodec();
    private RestTransitionWorkerResource resource;
    private PipelineOrchestratorConfig config;
    private PipelineOrchestratorConfig.RestWorkerConfig restConfig;
    private PipelineExecutionService executionService;
    private PipelineBundleIdentityResolver identityResolver;

    @BeforeEach
    void setUp() {
        resource = new RestTransitionWorkerResource();
        config = mock(PipelineOrchestratorConfig.class);
        restConfig = mock(PipelineOrchestratorConfig.RestWorkerConfig.class);
        executionService = mock(PipelineExecutionService.class);
        identityResolver = mock(PipelineBundleIdentityResolver.class);
        resource.orchestratorConfig = config;
        resource.executionService = executionService;
        resource.identityResolver = identityResolver;
        when(config.workerRest()).thenReturn(restConfig);
        when(restConfig.path()).thenReturn("/pipeline/worker/transitions/execute");
        when(restConfig.capabilitiesPath()).thenReturn("/pipeline/worker/capabilities");
        when(restConfig.sharedSecret()).thenReturn(java.util.Optional.of("worker-secret"));
        when(restConfig.sharedSecretRef()).thenReturn(java.util.Optional.empty());
        when(restConfig.signatureTolerance()).thenReturn(Duration.ofMinutes(2));
        when(identityResolver.pipelineId(config)).thenReturn("org.example.restaurant");
        when(identityResolver.bundleVersionId(config)).thenReturn("sha256:bundle");
        when(identityResolver.bundleHash()).thenReturn("bundle");
        when(identityResolver.capabilities()).thenReturn(PipelineBundleCapabilities.defaults());
    }

    @Test
    void rejectsCallsWhenServerDisabled() {
        when(restConfig.serverEnabled()).thenReturn(false);

        Response response = resource.execute(null, null, null, new byte[0]).await().indefinitely();

        assertEquals(404, response.getStatus());
    }

    @Test
    void delegatesToLocalExecutionServiceWhenEnabled() throws Exception {
        TransitionCommandEnvelope envelope = envelope();
        TransitionResultEnvelope result = TransitionResultEnvelope.completed(payloadCodec, List.of("ok"));
        when(restConfig.serverEnabled()).thenReturn(true);
        when(executionService.executePortableTransition(envelope))
            .thenReturn(Uni.createFrom().item(result));

        SignedRequest request = signed(envelope);
        Response response = resource.execute(
            request.timestamp(),
            request.nonce(),
            request.signature(),
            request.body()).await().indefinitely();

        assertEquals(200, response.getStatus());
        assertEquals(TransitionWorkerOutcome.COMPLETED, ((TransitionResultEnvelope) response.getEntity()).outcome());
    }

    @Test
    void delegatesWhenServerUsesSharedSecretReference() throws Exception {
        TransitionCommandEnvelope envelope = envelope();
        TransitionResultEnvelope result = TransitionResultEnvelope.completed(payloadCodec, List.of("ok"));
        when(restConfig.serverEnabled()).thenReturn(true);
        when(restConfig.sharedSecret()).thenReturn(java.util.Optional.empty());
        when(restConfig.sharedSecretRef()).thenReturn(java.util.Optional.of("sys:tpf.rest.worker.secret"));
        when(executionService.executePortableTransition(envelope))
            .thenReturn(Uni.createFrom().item(result));

        try {
            System.setProperty("tpf.rest.worker.secret", "worker-secret");
            SignedRequest request = signed(envelope);
            Response response = resource.execute(
                request.timestamp(),
                request.nonce(),
                request.signature(),
                request.body()).await().indefinitely();

            assertEquals(200, response.getStatus());
        } finally {
            System.clearProperty("tpf.rest.worker.secret");
        }
    }

    @Test
    void rejectsUnsignedRequestWhenServerEnabled() {
        when(restConfig.serverEnabled()).thenReturn(true);

        Response response = resource.execute(null, null, null, new byte[0]).await().indefinitely();

        assertEquals(401, response.getStatus());
    }

    @Test
    void startupValidationRejectsUnsupportedCustomServerPaths() {
        when(restConfig.serverEnabled()).thenReturn(true);
        when(restConfig.path()).thenReturn("/custom/execute");

        IllegalStateException error = assertThrows(IllegalStateException.class, resource::validateServerConfig);

        assertEquals(
            "REST transition worker server only supports "
                + "pipeline.orchestrator.worker.rest.path=/pipeline/worker/transitions/execute",
            error.getMessage());
    }

    @Test
    void returnsSignedWorkerCapabilitiesWhenEnabled() {
        when(restConfig.serverEnabled()).thenReturn(true);
        SignedRequest request = signedCapability();

        Response response = resource.capabilities(
            request.timestamp(),
            request.nonce(),
            request.signature()).await().indefinitely();

        assertEquals(200, response.getStatus());
        PipelineWorkerCapability capability = (PipelineWorkerCapability) response.getEntity();
        assertEquals("rest", capability.providerName());
        assertEquals("org.example.restaurant", capability.pipelineId());
        assertEquals("sha256:bundle", capability.bundleVersionId());
    }

    @Test
    void rejectsMissingServerSecret() throws Exception {
        when(restConfig.serverEnabled()).thenReturn(true);
        when(restConfig.sharedSecret()).thenReturn(java.util.Optional.empty());
        when(restConfig.sharedSecretRef()).thenReturn(java.util.Optional.empty());
        SignedRequest request = signed(envelope());

        Response response = resource.execute(
            request.timestamp(),
            request.nonce(),
            request.signature(),
            request.body()).await().indefinitely();

        assertEquals(503, response.getStatus());
    }

    @Test
    void rejectsReplayedNonce() throws Exception {
        TransitionCommandEnvelope envelope = envelope();
        TransitionResultEnvelope result = TransitionResultEnvelope.completed(payloadCodec, List.of("ok"));
        when(restConfig.serverEnabled()).thenReturn(true);
        when(executionService.executePortableTransition(envelope))
            .thenReturn(Uni.createFrom().item(result));
        SignedRequest request = signed(envelope);

        Response first = resource.execute(
            request.timestamp(),
            request.nonce(),
            request.signature(),
            request.body()).await().indefinitely();
        Response replay = resource.execute(
            request.timestamp(),
            request.nonce(),
            request.signature(),
            request.body()).await().indefinitely();

        assertEquals(200, first.getStatus());
        assertEquals(401, replay.getStatus());
    }

    private TransitionCommandEnvelope envelope() {
        return TransitionEnvelopeFixtures.envelope(payloadCodec);
    }

    private SignedRequest signed(TransitionCommandEnvelope envelope) throws Exception {
        byte[] body = PipelineJson.mapper().writeValueAsBytes(envelope);
        String timestamp = Instant.now().toString();
        String nonce = java.util.UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(
            "worker-secret",
            "POST",
            "/pipeline/worker/transitions/execute",
            timestamp,
            nonce,
            body);
        return new SignedRequest(timestamp, nonce, signature, body);
    }

    private SignedRequest signedCapability() {
        byte[] body = new byte[0];
        String timestamp = Instant.now().toString();
        String nonce = java.util.UUID.randomUUID().toString();
        String signature = TransitionWorkerSignature.sign(
            "worker-secret",
            "GET",
            "/pipeline/worker/capabilities",
            timestamp,
            nonce,
            body);
        return new SignedRequest(timestamp, nonce, signature, body);
    }

    private record SignedRequest(String timestamp, String nonce, String signature, byte[] body) {
    }
}
