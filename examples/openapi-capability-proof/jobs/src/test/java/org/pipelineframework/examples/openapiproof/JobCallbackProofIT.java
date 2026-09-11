package org.pipelineframework.examples.openapiproof;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import io.quarkus.test.InjectMock;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.command.CommandEffectStatus;
import org.pipelineframework.command.CommandEffectStore;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.http.HttpAuthorizationMaterial;
import org.pipelineframework.connector.http.HttpClientConnection;
import org.pipelineframework.examples.agentproof.domain.JobResult;
import org.pipelineframework.examples.agentproof.domain.StartJobRequest;
import org.pipelineframework.orchestrator.ExecutionStatus;

/** Generated native Command execution with the real HTTP callback admission resource. */
@QuarkusTest
@TestProfile(JobCallbackProofIT.CallbackProfile.class)
class JobCallbackProofIT {
    static final String KEY = "deterministic-callback-proof-signing-key";
    private static final Duration WAIT = Duration.ofSeconds(45);

    @InjectMock JobConnectionResolver connections;
    @InjectMock JobCallbackEndpointResolver endpoints;
    @Inject PipelineExecutionService execution;
    @Inject org.pipelineframework.orchestrator.PipelineControlPlane controlPlane;
    @Inject AwaitCoordinator coordinator;
    @Inject CommandEffectStore effects;
    @TestHTTPResource URI application;

    private final BlockingQueue<HttpRequest> requests = new LinkedBlockingQueue<>();
    private CompletableFuture<HttpResponse<byte[]>> acknowledgement;
    private HttpClient providerClient;
    private StartJobRequest input;

    public static class CallbackProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("pipeline.orchestrator.resume-token-secret", "callback-proof-resume-token-secret",
                "pipeline.callback.allow-http", "true", "openapi.proof.callback-signing-key", KEY,
                "quarkus.http.test-port", "0", "quarkus.otel.enabled", "false",
                "pipeline.orchestrator.sweep-interval", "PT1S",
                "pipeline.orchestrator.state-provider", "memory", "pipeline.command.effect-store.provider", "memory");
        }
    }

    @BeforeEach
    @SuppressWarnings({"rawtypes", "unchecked"})
    void hostPolicies() {
        requests.clear();
        acknowledgement = new CompletableFuture<>();
        input = new StartJobRequest(UUID.randomUUID().toString(), "preserved-business-input");
        when(endpoints.resolve(any())).thenReturn(application);
        HttpClient client = mock(HttpClient.class);
        providerClient = client;
        when(client.followRedirects()).thenReturn(HttpClient.Redirect.NEVER);
        when(client.sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenAnswer(call -> {
            requests.add(call.getArgument(0));
            return acknowledgement;
        });
        HttpClientConnection connection = new HttpClientConnection(client, URI.create("https://jobs-provider.example"),
            Set.of(), authorization -> CompletableFuture.completedStage(
                new HttpAuthorizationMaterial(Map.of(), Map.of(), Map.of())));
        when(connections.resolve(any())).thenReturn((CompletionStage) CompletableFuture.completedStage(connection));
    }

    @Test
    void waitsForSignedCallbackAndAcknowledgesDuplicateWithoutAnotherEffect() throws Exception {
        acknowledge();
        String submission = UUID.randomUUID().toString();
        String id = execution.executePipelineAsync(input, "default", submission).await().atMost(WAIT).executionId();
        URI callback = providerCallback();
        eventually(() -> status(id) == ExecutionStatus.WAITING_EXTERNAL);
        assertEquals(202, callback(callback, completion(), "application/json", true));
        assertResult(id);
        assertEquals(202, callback(callback, completion(), "application/json", true));
        assertTrue(execution.executePipelineAsync(input, "default", submission).await().atMost(WAIT).duplicate());
        assertTrue(requests.isEmpty());
        assertEquals(CommandEffectStatus.SUCCEEDED, effectStatus());
    }

    @Test
    void earlyCallbackIsObservedUntilProviderAcknowledgementSettlesDispatch() throws Exception {
        String id = submit();
        URI callback = providerCallback();
        assertEquals(202, callback(callback, completion(), "application/json", true));
        assertEquals(AwaitInteractionStatus.COMPLETION_OBSERVED,
            coordinator.resolveCallback(token(callback), System.currentTimeMillis()).await().atMost(WAIT).status());
        assertNotEquals(ExecutionStatus.SUCCEEDED, status(id));
        acknowledge();
        assertResult(id);
        assertTrue(requests.isEmpty());
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void deliberateRetryAfterDefinitePreSendFailurePreservesCommandAndCallbackIdentity() throws Exception {
        var attempts = new java.util.concurrent.CopyOnWriteArrayList<HttpRequest>();
        when(providerClient.sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenAnswer(call -> {
            HttpRequest request = call.getArgument(0);
            attempts.add(request);
            if (attempts.size() == 1) throw new IllegalStateException("client rejected dispatch before sending");
            requests.add(request);
            return acknowledgement;
        });
        acknowledge();
        String id = submit();
        eventually(() -> status(id).terminal());
        assertEquals(CommandEffectStatus.FAILED_RETRYABLE, effectStatus());
        assertEquals(1, attempts.size());
        assertTrue(requests.isEmpty(), "the failed attempt never reached the provider");
        long version = execution.getExecutionStatus("default", id).await().atMost(WAIT).version();
        controlPlane.redriveExecution("default", id, version, true,
            org.pipelineframework.orchestrator.ExecutionRedriveIntent.RETRY_FAILED_COMMAND,
            "Retry the proven pre-send failure").await().atMost(WAIT);
        URI callback = providerCallback();
        eventually(() -> status(id) == ExecutionStatus.WAITING_EXTERNAL);
        assertEquals(2, attempts.size());
        assertEquals(callbackFrom(attempts.getFirst()), callback);
        assertEquals(attempts.getFirst().headers().firstValue("Idempotency-Key"),
            attempts.getLast().headers().firstValue("Idempotency-Key"));
        assertEquals(202, callback(callback, completion(), "application/json", true));
        assertResult(id);
        assertEquals(2, effects.find("default", "job-" + input.jobId()).await().atMost(WAIT)
            .orElseThrow().attempts().size());
        assertTrue(requests.isEmpty(), "only the retried send reaches the provider");
    }

    @Test
    void ambiguousDispatchCompletesFromCallbackWithoutRewritingTheEffect() throws Exception {
        String id = submit();
        URI callback = providerCallback();
        acknowledgement.completeExceptionally(new IOException("provider connection closed after accepting request"));
        eventually(() -> status(id) == ExecutionStatus.WAITING_EXTERNAL);
        assertEquals(CommandEffectStatus.AMBIGUOUS, effectStatus());
        assertEquals(202, callback(callback, completion(), "application/json", true));
        assertResult(id);
        assertEquals(CommandEffectStatus.AMBIGUOUS, effectStatus());
        assertTrue(requests.isEmpty());
    }

    @Test
    void invalidAuthenticationTokenMediaAndSchemaDoNotCompleteTheWait() throws Exception {
        acknowledge();
        String id = submit();
        URI callback = providerCallback();
        eventually(() -> status(id) == ExecutionStatus.WAITING_EXTERNAL);
        assertEquals(400, callback(callback, completion(), "application/json", false));
        assertEquals(400, callback(URI.create(callback + "invalid"), completion(), "application/json", true));
        assertEquals(400, callback(callback, completion(), "text/plain", true));
        assertEquals(400, callback(callback, "{\"jobId\":\"missing-status\"}", "application/json", true));
        assertEquals(ExecutionStatus.WAITING_EXTERNAL, status(id));
        assertEquals(202, callback(callback, completion(), "application/json", true));
        assertResult(id);
    }

    @Test
    void ambiguityWithoutCallbackReachesAwaitTimeout() throws Exception {
        String id = submit();
        URI callback = providerCallback();
        acknowledgement.completeExceptionally(new IOException("provider connection closed after send"));
        eventually(() -> status(id).terminal());
        assertNotEquals(ExecutionStatus.SUCCEEDED, status(id));
        assertEquals(CommandEffectStatus.AMBIGUOUS, effectStatus());
        assertEquals(400, callback(callback, completion(), "application/json", true));
    }

    private String submit() {
        return execution.executePipelineAsync(input, "default", UUID.randomUUID().toString())
            .await().atMost(WAIT).executionId();
    }

    private ExecutionStatus status(String id) {
        return execution.getExecutionStatus("default", id).await().atMost(WAIT).status();
    }

    private CommandEffectStatus effectStatus() {
        return effects.find("default", "job-" + input.jobId()).await().atMost(WAIT).orElseThrow().status();
    }

    private void assertResult(String id) throws Exception {
        eventually(() -> status(id) == ExecutionStatus.SUCCEEDED);
        assertEquals(new JobResult(input.jobId(), input.value(), "completed"),
            execution.getExecutionResult("default", id, JobResult.class, false).await().atMost(WAIT));
    }

    private void acknowledge() {
        @SuppressWarnings("unchecked") HttpResponse<byte[]> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(202);
        when(response.uri()).thenReturn(URI.create("https://jobs-provider.example/jobs"));
        when(response.headers()).thenReturn(HttpHeaders.of(Map.of("Content-Type", List.of("application/json")),
            (name, value) -> true));
        when(response.body()).thenReturn(("{\"jobId\":\"" + input.jobId() + "\",\"accepted\":true}")
            .getBytes(StandardCharsets.UTF_8));
        acknowledgement.complete(response);
    }

    private URI providerCallback() throws Exception {
        HttpRequest request = requests.poll(10, TimeUnit.SECONDS);
        assertNotNull(request, "native Command did not reach the provider");
        return callbackFrom(request);
    }

    private URI callbackFrom(HttpRequest request) throws Exception {
        assertEquals("/jobs", request.uri().getPath());
        assertTrue(request.headers().firstValue("Idempotency-Key").isPresent());
        var bytes = new ByteArrayOutputStream();
        var completed = new CompletableFuture<Boolean>();
        request.bodyPublisher().orElseThrow().subscribe(new Flow.Subscriber<ByteBuffer>() {
            public void onSubscribe(Flow.Subscription subscription) { subscription.request(Long.MAX_VALUE); }
            public void onNext(ByteBuffer buffer) {
                byte[] value = new byte[buffer.remaining()]; buffer.get(value); bytes.writeBytes(value);
            }
            public void onError(Throwable failure) { completed.completeExceptionally(failure); }
            public void onComplete() { completed.complete(true); }
        });
        completed.get(5, TimeUnit.SECONDS);
        var wire = PipelineJson.mapper().readTree(bytes.toByteArray());
        assertEquals(input.jobId(), wire.required("jobId").textValue());
        assertEquals(input.value(), wire.required("value").textValue());
        assertEquals(3, wire.size(), "only TPF adds the callback field");
        return URI.create(wire.required("callbackUrl").textValue());
    }

    private String completion() { return "{\"jobId\":\"" + input.jobId() + "\",\"status\":\"completed\"}"; }

    private String token(URI callback) { return callback.getPath().substring(callback.getPath().lastIndexOf('/') + 1); }

    private int callback(URI uri, String body, String media, boolean validSignature) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(KEY.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        String signature = validSignature ? HexFormat.of().formatHex(mac.doFinal(body.getBytes(StandardCharsets.UTF_8)))
            : "0".repeat(64);
        try (HttpClient client = HttpClient.newHttpClient()) {
            return client.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5))
                .header("Content-Type", media).header("X-Callback-Signature", signature)
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.discarding()).statusCode();
        }
    }

    private void eventually(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(20);
        assertTrue(condition.getAsBoolean(), "execution did not reach the expected state within " + WAIT);
    }
}
