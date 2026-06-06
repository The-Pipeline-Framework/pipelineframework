package org.pipelineframework.orchestrator;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.invocation.TransportBoundaryInvocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RestPipelineTransitionWorkerTest {

    private final JsonTransitionPayloadCodec payloadCodec = new JsonTransitionPayloadCodec();
    private HttpServer server;
    private RestPipelineTransitionWorker worker;
    private PipelineOrchestratorConfig config;
    private PipelineOrchestratorConfig.RestWorkerConfig restConfig;
    private HttpClient client;

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.start();
        config = mock(PipelineOrchestratorConfig.class);
        restConfig = mock(PipelineOrchestratorConfig.RestWorkerConfig.class);
        when(config.workerRest()).thenReturn(restConfig);
        when(restConfig.baseUrl()).thenReturn(Optional.of("http://localhost:" + server.getAddress().getPort()));
        when(restConfig.path()).thenReturn("/pipeline/worker/transitions/execute");
        when(restConfig.capabilitiesPath()).thenReturn("/pipeline/worker/capabilities");
        when(restConfig.requestTimeout()).thenReturn(Duration.ofSeconds(5));
        when(restConfig.sharedSecret()).thenReturn(Optional.of("worker-secret"));
        when(restConfig.sharedSecretRef()).thenReturn(Optional.empty());
        client = HttpClient.newHttpClient();
        worker = new RestPipelineTransitionWorker(client);
        worker.orchestratorConfig = config;
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void postsCommandEnvelopeAndDecodesCompletedResult() throws Exception {
        AtomicReference<String> requestBody = new AtomicReference<>();
        AtomicReference<String> signature = new AtomicReference<>();
        server.createContext("/pipeline/worker/transitions/execute", exchange -> {
            requestBody.set(new String(exchange.getRequestBody().readAllBytes()));
            signature.set(exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.SIGNATURE_HEADER));
            byte[] response = PipelineJson.mapper()
                .writeValueAsBytes(TransitionResultEnvelope.completed(payloadCodec, List.of("ok")));
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(response);
            }
        });

        TransitionResultEnvelope result = worker.executeTransition(envelope()).await().indefinitely();

        assertEquals(TransitionWorkerOutcome.COMPLETED, result.outcome());
        assertEquals(List.of("ok"), result.decodeOutputItems(payloadCodec));
        assertTrue(requestBody.get().contains("\"transitionKey\":\"exec-1:0:0\""));
        assertTrue(signature.get() != null && !signature.get().isBlank());
    }

    @Test
    void exposesTransportBoundaryDiagnostics() {
        assertTrue(worker instanceof TransportBoundaryInvocation);
        assertEquals("rest", worker.transportBoundary().protocol());
        assertEquals("transition-worker.execute", worker.transportBoundary().target());
    }

    @Test
    void signsRequestWithSharedSecretReference() throws Exception {
        AtomicReference<String> signature = new AtomicReference<>();
        when(restConfig.sharedSecret()).thenReturn(Optional.empty());
        when(restConfig.sharedSecretRef()).thenReturn(Optional.of("sys:tpf.rest.worker.secret"));
        System.setProperty("tpf.rest.worker.secret", "worker-secret");
        server.createContext("/pipeline/worker/transitions/execute", exchange -> {
            exchange.getRequestBody().readAllBytes();
            signature.set(exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.SIGNATURE_HEADER));
            byte[] response = PipelineJson.mapper()
                .writeValueAsBytes(TransitionResultEnvelope.completed(payloadCodec, List.of("ok")));
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(response);
            }
        });

        try {
            TransitionResultEnvelope result = worker.executeTransition(envelope()).await().indefinitely();

            assertEquals(TransitionWorkerOutcome.COMPLETED, result.outcome());
            assertTrue(signature.get() != null && !signature.get().isBlank());
        } finally {
            System.clearProperty("tpf.rest.worker.secret");
        }
    }

    @Test
    void fetchesSignedWorkerCapabilities() throws Exception {
        AtomicReference<String> signature = new AtomicReference<>();
        server.createContext("/pipeline/worker/capabilities", exchange -> {
            signature.set(exchange.getRequestHeaders().getFirst(TransitionWorkerSignature.SIGNATURE_HEADER));
            byte[] response = PipelineJson.mapper().writeValueAsBytes(new PipelineWorkerCapability(
                PipelineWorkerCapability.PROTOCOL_VERSION,
                "rest",
                "org.example.restaurant",
                "sha256:bundle",
                "bundle",
                List.of("application/tpf-transition-envelope+json"),
                List.of("rest")));
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(response);
            }
        });

        PipelineWorkerCapability capability = worker.capabilities().await().indefinitely();

        assertEquals("rest", capability.providerName());
        assertEquals("org.example.restaurant", capability.pipelineId());
        assertEquals("sha256:bundle", capability.bundleVersionId());
        assertTrue(signature.get() != null && !signature.get().isBlank());
    }

    @Test
    void failsOnNon2xxResponse() {
        server.createContext("/pipeline/worker/transitions/execute", exchange -> {
            exchange.getRequestBody().readAllBytes();
            byte[] response = "nope".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(503, response.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(response);
            }
        });

        TransitionWorkerFailureException error = assertThrows(
            TransitionWorkerFailureException.class,
            () -> worker.executeTransition(envelope()).await().indefinitely());

        assertTrue(error.getMessage().contains("HTTP 503"));
    }

    @Test
    void failsOnMalformedJson() {
        server.createContext("/pipeline/worker/transitions/execute", exchange -> {
            exchange.getRequestBody().readAllBytes();
            byte[] response = "{not-json".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(response);
            }
        });

        TransitionWorkerFailureException error = assertThrows(
            TransitionWorkerFailureException.class,
            () -> worker.executeTransition(envelope()).await().indefinitely());

        assertTrue(error.getMessage().contains("malformed JSON"));
    }

    @Test
    void rejectsRelativeBaseUrlAtStartupValidation() {
        when(restConfig.baseUrl()).thenReturn(Optional.of("worker/execute"));
        when(restConfig.isEnabled()).thenReturn(true);

        Optional<String> error = worker.startupValidationError(config);

        assertTrue(error.isPresent());
        assertTrue(error.get().contains("Invalid pipeline.orchestrator.worker.rest.base-url"));
    }

    @Test
    void requiresSharedSecretAtStartupValidation() {
        when(restConfig.isEnabled()).thenReturn(true);
        when(restConfig.sharedSecret()).thenReturn(Optional.empty());
        when(restConfig.sharedSecretRef()).thenReturn(Optional.empty());

        Optional<String> error = worker.startupValidationError(config);

        assertTrue(error.isPresent());
        assertTrue(error.get().contains("pipeline.orchestrator.worker.rest.shared-secret"));
    }

    @Test
    void acceptsSharedSecretReferenceAtStartupValidation() {
        when(restConfig.isEnabled()).thenReturn(true);
        when(restConfig.sharedSecret()).thenReturn(Optional.empty());
        when(restConfig.sharedSecretRef()).thenReturn(Optional.of("env:TPF_REST_WORKER_SECRET"));

        Optional<String> error = worker.startupValidationError(config);

        assertTrue(error.isEmpty());
    }

    @Test
    void rejectsAmbiguousSharedSecretAtStartupValidation() {
        when(restConfig.isEnabled()).thenReturn(true);
        when(restConfig.sharedSecret()).thenReturn(Optional.of("worker-secret"));
        when(restConfig.sharedSecretRef()).thenReturn(Optional.of("env:TPF_REST_WORKER_SECRET"));

        Optional<String> error = worker.startupValidationError(config);

        assertTrue(error.isPresent());
        assertTrue(error.get().contains("Configure only one"));
    }

    @Test
    void failsBeforeRequestWhenSharedSecretIsMissing() {
        when(restConfig.sharedSecret()).thenReturn(Optional.empty());
        when(restConfig.sharedSecretRef()).thenReturn(Optional.empty());

        IllegalStateException error = assertThrows(
            IllegalStateException.class,
            () -> worker.executeTransition(envelope()).await().indefinitely());

        assertTrue(error.getMessage().contains("shared-secret"));
    }

    private TransitionCommandEnvelope envelope() {
        return TransitionEnvelopeFixtures.envelope(payloadCodec);
    }
}
