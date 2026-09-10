package org.pipelineframework.examples.openapiproof;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.http.HttpAuthorizationMaterial;
import org.pipelineframework.connector.http.HttpClientConnection;
import org.pipelineframework.examples.agentproof.connector.ProofInvocationRecorder;
import org.pipelineframework.examples.agentproof.domain.AgentState;
import org.pipelineframework.examples.agentproof.domain.ApplicationResult;
import org.pipelineframework.examples.agentproof.domain.LookupArguments;
import org.pipelineframework.examples.agentproof.domain.LookupResult;
import org.pipelineframework.examples.agentproof.domain.RecordArguments;
import org.pipelineframework.examples.agentproof.domain.RecordResult;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.step.StepOneToOne;

@QuarkusTest
class OpenApiCapabilityProofIT {
    @InjectMock EvidenceApiConnectionResolver connectionResolver;
    @Inject @Any Instance<StepOneToOne<AgentState, ApplicationResult>> callablePipelines;
    @Inject @Any Instance<StepOneToOne<LookupArguments, LookupResult>> lookupPipelines;
    @Inject @Any Instance<StepOneToOne<RecordArguments, RecordResult>> recordPipelines;
    @Inject ProofInvocationRecorder recorder;
    @Inject PipelineConfig config;
    @Inject PipelineInvocationRuntime invocationRuntime;

    private final List<HttpRequest> requests = new ArrayList<>();
    private HttpClient client;
    private String executionId;

    @BeforeEach
    @SuppressWarnings({ "rawtypes", "unchecked" })
    void configureHostOwnedConnection() {
        recorder.reset();
        requests.clear();
        config.maxRecursiveDepth(4);
        client = mock(HttpClient.class);
        when(client.followRedirects()).thenReturn(HttpClient.Redirect.NEVER);
        when(client.sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenAnswer(invocation -> {
            HttpRequest request = invocation.getArgument(0);
            requests.add(request);
            return CompletableFuture.completedStage(request.uri().getPath().endsWith("/lookup")
                ? response(200, "{\"kind\":\"found\",\"payload\":{\"value\":\"verified\"}}")
                : response(202, "{\"receipt\":\"recorded\"}"));
        });
        HttpClientConnection connection = new HttpClientConnection(client, URI.create("https://runtime-owned.example/v1"),
            Set.of("oauth2", "apiKey"), authorization -> CompletableFuture.completedStage(
                authorization.security().requirements().stream().anyMatch(value -> value.scheme().equals("oauth2"))
                    ? new HttpAuthorizationMaterial(Map.of("Authorization", List.of("Bearer host-token")), Map.of(), Map.of())
                    : new HttpAuthorizationMaterial(Map.of("X-Api-Key", List.of("host-key")), Map.of(), Map.of())));
        when(connectionResolver.resolve(any())).thenReturn((CompletionStage) CompletableFuture.completedStage(connection));
        executionId = "openapi-proof-" + UUID.randomUUID();
        setExecutionContext();
    }

    @AfterEach
    void clearContext() {
        PipelineExecutionContextHolder.clear();
    }

    @Test
    void importedCapabilitiesRunThroughTheExistingPackagedCallableLoopAndReplayNatively() {
        AgentState state = new AgentState("tenant-a/evidence", "effect-1", "none", "lookup");

        ApplicationResult first = invoke(callablePipeline(), state).await().indefinitely();
        setExecutionContext();
        ApplicationResult replay = invoke(callablePipeline(), state).await().indefinitely();

        assertEquals(first, replay);
        assertEquals(new ApplicationResult("query then command completed", 3), first);
        assertEquals(2, requests.size(), "captured Query and recorded Command must not redispatch on replay");
        HttpRequest lookup = requests.getFirst();
        HttpRequest record = requests.getLast();
        assertEquals("POST", lookup.method());
        assertEquals("https", lookup.uri().getScheme());
        assertEquals("runtime-owned.example", lookup.uri().getHost());
        assertEquals("/v1/evidence/missing-proof/lookup", lookup.uri().getPath());
        assertEquals("verbose=true", lookup.uri().getQuery());
        assertEquals("Bearer host-token", lookup.headers().firstValue("Authorization").orElseThrow());
        assertJsonEquals("{\"key\":\"evidence\"}", body(lookup));
        assertEquals("host-key", record.headers().firstValue("X-Api-Key").orElseThrow());
        assertTrue(record.headers().firstValue("Idempotency-Key").orElseThrow().length() >= 16);
        assertJsonEquals("{\"effect_key\":\"effect-1\",\"value\":\"record-proof\"}", body(record));
        verify(client, times(2)).sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
    }

    @Test
    void importedOperationsAreAlsoOrdinaryDirectPipelines() {
        LookupResult lookup = invoke(lookupPipeline(), new LookupArguments("customer-7")).await().indefinitely();
        setExecutionContext("direct-command-" + UUID.randomUUID());
        RecordResult record = invoke(recordPipeline(), new RecordArguments("record-direct", "effect-direct"))
            .await().indefinitely();

        assertEquals(new LookupResult("verified"), lookup);
        assertEquals(new RecordResult("recorded"), record);
        assertEquals(2, requests.size());
    }

    @Test
    void generatedReleaseMetadataPinsSourceMappingsAndAuthorityWithoutRuntimeSecrets() throws Exception {
        String contract = metadata("pipeline-contract.json");
        String bindings = metadata("connector-bindings.json");
        String all = contract + bindings;

        assertTrue(contract.contains("\"capabilityImports\""), contract);
        assertTrue(contract.contains("\"sourceKind\": \"OPENAPI\""), contract);
        assertTrue(contract.contains("evidence.lookup"), contract);
        assertTrue(contract.contains("evidence.record"), contract);
        assertTrue(contract.contains("mappingFingerprint"), contract);
        assertTrue(bindings.contains("\"schemaVersion\": 3"), bindings);
        assertFalse(all.contains("descriptive-only.invalid"), all);
        assertFalse(all.contains("runtime-owned.example"), all);
        assertFalse(all.contains("host-token"), all);
        assertFalse(all.contains("host-key"), all);
        assertFalse(all.contains("tenant-a/evidence"), all);
        assertNull(Thread.currentThread().getContextClassLoader().getResource("contract/openapi.yaml"));
        assertNull(Thread.currentThread().getContextClassLoader().getResource("openapi.yaml"));
        assertNotNull(Thread.currentThread().getContextClassLoader()
            .getResource("META-INF/pipeline/http-operations.json"));
    }

    @Test
    void consumerOwnsOnlyItsHostConnectionBoundary() throws Exception {
        Path sources = Path.of(System.getProperty("user.dir")).resolve("src/main/java");
        try (var files = Files.walk(sources)) {
            assertEquals(List.of("EvidenceApiConnectionResolver.java"), files.filter(Files::isRegularFile)
                .map(path -> path.getFileName().toString()).sorted().toList());
        }
    }

    private StepOneToOne<AgentState, ApplicationResult> callablePipeline() {
        return callablePipelines.stream().filter(candidate -> Arrays.stream(candidate.getClass().getDeclaredFields())
            .anyMatch(field -> field.getName().equals("child3") && !field.getType().equals(Provider.class)))
            .findFirst().orElseThrow();
    }

    private StepOneToOne<LookupArguments, LookupResult> lookupPipeline() {
        return lookupPipelines.stream().filter(candidate -> candidate.getClass().getName().contains("EvidenceLookup"))
            .findFirst().orElseThrow(() -> new AssertionError("generated direct OpenAPI Query pipeline not found: "
                + lookupPipelines.stream().map(value -> value.getClass().getName()).toList()));
    }

    private StepOneToOne<RecordArguments, RecordResult> recordPipeline() {
        return recordPipelines.stream().filter(candidate -> candidate.getClass().getName().contains("EvidenceCommand"))
            .findFirst().orElseThrow(() -> new AssertionError("generated direct OpenAPI Command pipeline not found: "
                + recordPipelines.stream().map(value -> value.getClass().getName()).toList()));
    }

    private <I, O> io.smallrye.mutiny.Uni<O> invoke(StepOneToOne<I, O> pipeline, I input) {
        return invocationRuntime.invokeStepUni(null, null, () -> pipeline.applyOneToOne(input));
    }

    private void setExecutionContext() {
        setExecutionContext(executionId);
    }

    private static void setExecutionContext(String id) {
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("tenant-a", id, 0));
    }

    private static HttpResponse<byte[]> response(int status, String json) {
        @SuppressWarnings("unchecked") HttpResponse<byte[]> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(status);
        when(response.headers()).thenReturn(HttpHeaders.of(
            Map.of("Content-Type", List.of("application/json")), (left, right) -> true));
        when(response.body()).thenReturn(json.getBytes(StandardCharsets.UTF_8));
        when(response.uri()).thenReturn(URI.create("https://runtime-owned.example/v1/evidence/response"));
        return response;
    }

    private static String body(HttpRequest request) {
        HttpRequest.BodyPublisher publisher = request.bodyPublisher().orElseThrow();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        CompletableFuture<Void> done = new CompletableFuture<>();
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override public void onSubscribe(Flow.Subscription subscription) { subscription.request(Long.MAX_VALUE); }
            @Override public void onNext(ByteBuffer item) {
                byte[] bytes = new byte[item.remaining()];
                item.get(bytes);
                output.writeBytes(bytes);
            }
            @Override public void onError(Throwable throwable) { done.completeExceptionally(throwable); }
            @Override public void onComplete() { done.complete(null); }
        });
        done.join();
        return output.toString(StandardCharsets.UTF_8);
    }

    private static void assertJsonEquals(String expected, String actual) {
        try {
            assertEquals(PipelineJson.mapper().readTree(expected), PipelineJson.mapper().readTree(actual));
        } catch (java.io.IOException failure) {
            throw new AssertionError("HTTP proof emitted invalid JSON", failure);
        }
    }

    private static String metadata(String name) throws Exception {
        try (InputStream stream = Thread.currentThread().getContextClassLoader()
            .getResourceAsStream("META-INF/pipeline/" + name)) {
            if (stream == null) throw new AssertionError("generated metadata resource not found: " + name);
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
