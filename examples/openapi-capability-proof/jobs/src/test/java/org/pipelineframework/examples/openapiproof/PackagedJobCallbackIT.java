package org.pipelineframework.examples.openapiproof;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarFile;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.command.CommandEffectStatus;
import org.pipelineframework.command.DynamoCommandEffectStore;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** A real packaged JVM resumes a durable callback wait after being stopped and restarted. */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PackagedJobCallbackIT {
    @Container
    static final LocalStackContainer DYNAMO = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
        .withServices("dynamodb");
    private static final String SIGNING_KEY = "packaged-proof-provider-signing-key";
    private static final String ADMIN = "packaged-proof-control-token";
    private static final Duration WAIT = Duration.ofSeconds(45);
    private final Path module = Path.of(System.getProperty("user.dir")).toAbsolutePath();
    private final HttpClient client = HttpClient.newHttpClient();
    private final java.util.concurrent.ExecutorService providerThreads = Executors.newVirtualThreadPerTaskExecutor();
    private final ArrayBlockingQueue<Dispatch> dispatches = new ArrayBlockingQueue<>(8);
    private final AtomicInteger providerEffects = new AtomicInteger();
    private Optional<Process> application = Optional.empty();
    private Optional<HttpServer> provider = Optional.empty();
    private Optional<DynamoDbClient> database = Optional.empty();
    private int appPort;
    private int starts;
    @TempDir Path workingDirectory;

    @BeforeAll
    void infrastructure() throws Exception {
        DynamoDbClient dynamo = DynamoDbClient.builder().endpointOverride(DYNAMO.getEndpoint())
            .region(Region.of(DYNAMO.getRegion())).credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(DYNAMO.getAccessKey(), DYNAMO.getSecretKey()))).build();
        database = Optional.of(dynamo);
        new DynamoProofTables(dynamo).create();
        try (ServerSocket socket = new ServerSocket(0)) { appPort = socket.getLocalPort(); }
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        provider = Optional.of(server);
        server.setExecutor(providerThreads);
        server.createContext("/jobs", exchange -> {
            try (exchange) {
                JsonNode request = PipelineJson.mapper().readTree(exchange.getRequestBody().readAllBytes());
                providerEffects.incrementAndGet();
                dispatches.add(new Dispatch(URI.create(request.required("callbackUrl").textValue()), request,
                    exchange.getRequestHeaders().getFirst("Idempotency-Key")));
                byte[] response = PipelineJson.mapper().writeValueAsBytes(Map.of(
                    "jobId", request.required("jobId").textValue(), "accepted", true));
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(202, response.length);
                exchange.getResponseBody().write(response);
            }
        });
        server.start();
    }

    @AfterAll
    void cleanup() throws Exception {
        stop();
        provider.ifPresent(server -> server.stop(0));
        providerThreads.close();
        database.ifPresent(DynamoDbClient::close);
        client.close();
    }

    @Test
    void packagedApplicationResumesAfterRestartWithoutSourceParserOrRedispatch() throws Exception {
        assertRuntimeIsOffline();
        start();
        String jobId = UUID.randomUUID().toString();
        String submissionKey = "submission-" + jobId;
        String payload = PipelineJson.mapper().writeValueAsString(Map.of("jobId", jobId, "value", "original-value"));
        String submission = PipelineJson.mapper().writeValueAsString(Map.of(
            "pipelineId", pipelineId(), "inputShape", "UNI", "idempotencyKey", submissionKey,
            "inputPayload", Map.of("payloadTypeId", "org.pipelineframework.examples.agentproof.domain.StartJobRequest",
                "payloadEncoding", "application/tpf-transition+json", "payload", payload)));
        JsonNode accepted = json(request("POST", "/executions", submission), 200);
        String id = accepted.required("executionId").textValue();
        Dispatch dispatch = dispatches.poll(15, TimeUnit.SECONDS);
        assertNotNull(dispatch, "packaged native Command never reached the provider");
        assertEquals(3, dispatch.request().size());
        assertEquals(jobId, dispatch.request().required("jobId").textValue());
        assertEquals(appPort, dispatch.callback().getPort());
        awaitStatus(id, "WAITING_EXTERNAL");
        var effects = new DynamoCommandEffectStore(database.orElseThrow(), "tpf_command_effect");
        var before = effects.find("default", "job-" + jobId).await().atMost(WAIT).orElseThrow();
        assertEquals(CommandEffectStatus.SUCCEEDED, before.status());
        assertEquals(1, before.attempts().size());
        assertEquals(before.attempts().getFirst().occurrenceId(), dispatch.idempotencyKey());

        stop();
        start();
        assertEquals("WAITING_EXTERNAL", status(id).required("status").textValue());
        String completion = PipelineJson.mapper().writeValueAsString(Map.of("jobId", jobId, "status", "completed"));
        assertEquals(202, complete(dispatch.callback(), completion));
        awaitStatus(id, "SUCCEEDED");
        JsonNode result = json(request("GET", "/executions/" + id + "/result", ""), 200);
        JsonNode value = PipelineJson.mapper().readTree(result.required("resultPayload").required("payload").textValue());
        assertEquals(jobId, value.required("jobId").textValue());
        assertEquals("original-value", value.required("value").textValue());
        assertEquals("completed", value.required("status").textValue());
        long version = status(id).required("version").longValue();
        assertEquals(202, complete(dispatch.callback(), completion));
        assertEquals(version, status(id).required("version").longValue());
        JsonNode replay = json(request("POST", "/executions", submission), 200);
        assertTrue(replay.required("duplicate").booleanValue());
        assertEquals(id, replay.required("executionId").textValue());
        assertEquals(before, effects.find("default", "job-" + jobId).await().atMost(WAIT).orElseThrow());
        assertEquals(1, providerEffects.get());
        assertTrue(dispatches.isEmpty());
    }

    private void start() throws Exception {
        Path log = module.resolve("target/packaged-callback-" + (++starts) + ".log");
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Dquarkus.http.port=" + appPort, "-Dquarkus.otel.sdk.disabled=true",
            "-Dpipeline.orchestrator.control-plane.enabled=true",
            "-Dpipeline.orchestrator.control-plane.admin-token=" + ADMIN,
            "-Dpipeline.orchestrator.admin.enabled=true", "-Dpipeline.orchestrator.admin.admin-token=" + ADMIN,
            "-Dpipeline.orchestrator.resume-token-secret=packaged-proof-stable-resume-secret",
            "-Dpipeline.orchestrator.sweep-interval=PT1S",
            "-Dpipeline.orchestrator.dynamo.region=" + DYNAMO.getRegion(),
            "-Dpipeline.orchestrator.dynamo.endpoint-override=" + DYNAMO.getEndpoint(),
            "-Dquarkus.dynamodb.aws.region=" + DYNAMO.getRegion(),
            "-Dquarkus.dynamodb.endpoint-override=" + DYNAMO.getEndpoint(),
            "-Dquarkus.dynamodb.aws.credentials.type=static",
            "-Dquarkus.dynamodb.aws.credentials.static-provider.access-key-id=" + DYNAMO.getAccessKey(),
            "-Dquarkus.dynamodb.aws.credentials.static-provider.secret-access-key=" + DYNAMO.getSecretKey(),
            "-Dpipeline.callback.allow-http=true", "-Dopenapi.proof.callback-base-uri=http://127.0.0.1:" + appPort,
            "-Dopenapi.proof.callback-signing-key=" + SIGNING_KEY,
            "-Dopenapi.proof.jobs-base-uri=http://127.0.0.1:" + provider.orElseThrow().getAddress().getPort(),
            "-jar", module.resolve("target/quarkus-app/quarkus-run.jar").toString()));
        ProcessBuilder builder = new ProcessBuilder(command).directory(workingDirectory.toFile())
            .redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().putAll(Map.of("AWS_ACCESS_KEY_ID", DYNAMO.getAccessKey(),
            "AWS_SECRET_ACCESS_KEY", DYNAMO.getSecretKey(), "AWS_REGION", DYNAMO.getRegion()));
        Process process = builder.start();
        application = Optional.of(process);
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (process.isAlive() && System.nanoTime() < deadline) {
            try {
                if (client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + appPort + "/q/health/ready"))
                    .timeout(Duration.ofSeconds(1)).GET().build(), HttpResponse.BodyHandlers.discarding()).statusCode() == 200) {
                    registerLocalWorker();
                    return;
                }
            } catch (IOException unavailable) { /* The process has not bound its listener yet. */ }
            Thread.sleep(50);
        }
        fail("Packaged process did not become ready; inspect " + log);
    }

    private void stop() throws Exception {
        if (application.isEmpty()) return;
        Process process = application.orElseThrow();
        process.destroy();
        if (!process.waitFor(10, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            assertTrue(process.waitFor(5, TimeUnit.SECONDS), "packaged JVM did not stop");
        }
        application = Optional.empty();
    }

    private HttpResponse<String> request(String method, String path, String body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + appPort
                + "/tpf/control-plane/tenants/default" + path)).timeout(Duration.ofSeconds(10))
            .header("Authorization", "Bearer " + ADMIN).header("Content-Type", "application/json")
            .method(method, body.isEmpty() ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
            .build(), HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode json(HttpResponse<String> response, int status) throws Exception {
        assertEquals(status, response.statusCode(), response.body());
        return PipelineJson.mapper().readTree(response.body());
    }

    private JsonNode status(String id) throws Exception { return json(request("GET", "/executions/" + id, ""), 200); }

    private String pipelineId() throws IOException {
        return PipelineJson.mapper().readTree(Files.readString(
            module.resolve("target/classes/META-INF/pipeline/pipeline-contract.json"))).required("pipelineId").textValue();
    }

    private void registerLocalWorker() throws Exception {
        JsonNode contract = PipelineJson.mapper().readTree(Files.readString(
            module.resolve("target/classes/META-INF/pipeline/pipeline-contract.json")));
        String version = contract.required("contractVersion").textValue();
        String body = PipelineJson.mapper().writeValueAsString(Map.of("workerId", "packaged-worker",
            "contractVersion", version, "releaseVersion", version, "protocol", "local",
            "endpoint", "local://callback-proof", "artifactId", "", "artifactDigest", ""));
        json(client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + appPort
                + "/tpf/admin/tenants/default/pipelines/" + pipelineId() + "/workers/register"))
            .header("Authorization", "Bearer " + ADMIN).header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString()), 200);
    }

    private void awaitStatus(String id, String expected) throws Exception {
        long deadline = System.nanoTime() + WAIT.toNanos();
        JsonNode state = status(id);
        while (!expected.equals(state.required("status").textValue()) && System.nanoTime() < deadline) {
            Thread.sleep(30); state = status(id);
        }
        assertEquals(expected, state.required("status").textValue(), state.toString());
    }

    private int complete(URI callback, String body) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(SIGNING_KEY.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return client.send(HttpRequest.newBuilder(callback).timeout(Duration.ofSeconds(10))
            .header("Content-Type", "application/json").header("X-Callback-Signature",
                HexFormat.of().formatHex(mac.doFinal(body.getBytes(StandardCharsets.UTF_8))))
            .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    private void assertRuntimeIsOffline() throws Exception {
        try (var paths = Files.walk(module.resolve("target/quarkus-app"))) {
            for (Path path : paths.filter(file -> file.toString().endsWith(".jar")).toList()) {
                try (JarFile jar = new JarFile(path.toFile())) {
                    assertNull(jar.getEntry("io/swagger/v3/parser/OpenAPIV3Parser.class"), path.toString());
                    for (String source : List.of("openapi.yaml", "contract/openapi.yaml", "contract/schemas.yaml"))
                        assertNull(jar.getEntry(source), path + " contains source contract " + source);
                }
            }
        }
        try (var paths = Files.list(workingDirectory)) { assertEquals(0, paths.count()); }
    }

    private record Dispatch(URI callback, JsonNode request, String idempotencyKey) {}
}
