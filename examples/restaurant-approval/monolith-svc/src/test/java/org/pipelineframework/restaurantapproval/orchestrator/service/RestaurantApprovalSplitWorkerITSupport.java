package org.pipelineframework.restaurantapproval.orchestrator.service;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import io.restassured.specification.RequestSpecification;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.jar.JarFile;
import org.junit.jupiter.api.AfterEach;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.dto.AwaitCompletionResponseDto;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

abstract class RestaurantApprovalSplitWorkerITSupport {

    protected static final String TENANT_ID = "restaurant-demo";
    protected static final String PIPELINE_ID = "org.pipelineframework.restaurantapproval";
    protected static final String AWAIT_STEP_ID = "ProcessAwaitRestaurantDecisionService";
    protected static final String WORKER_SECRET = "restaurant-transition-worker-secret";
    protected static final String CONTROL_PLANE_ADMIN_TOKEN = "restaurant-control-plane-admin-token";
    private static final String PAYLOAD_ENCODING = "application/tpf-transition+json";
    private static final String ORDER_REQUEST_DTO_TYPE =
        "org.pipelineframework.restaurantapproval.common.dto.PlaceRestaurantOrderRequestDto";

    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(60);
    private static final ObjectMapper JSON = new ObjectMapper().findAndRegisterModules();

    private final List<Process> processes = new ArrayList<>();

    @AfterEach
    void stopProcesses() {
        for (Process process : processes) {
            stopProcess(process);
        }
    }

    protected final void assertAcceptedAndDeclinedFlows(int coordinatorPort) throws InterruptedException {
        assertAcceptedFlow(coordinatorPort, false);
        assertDeclinedFlow(coordinatorPort, false);
    }

    protected final void assertHostedAcceptedAndDeclinedFlows(int coordinatorPort) throws InterruptedException {
        assertAcceptedFlow(coordinatorPort, true);
        assertDeclinedFlow(coordinatorPort, true);
    }

    protected final void assertHostedSubmitStatus(int coordinatorPort, int statusCode) {
        submitOrderRequest(coordinatorPort, "Alan Turing", "Mismatched Bundle Cafe", true)
            .statusCode(statusCode);
    }

    protected final void registerAndActivateHostedBundle(int coordinatorPort) throws IOException {
        Path releaseJar = locateReleaseArtifact();
        Map<String, Object> contract = readPipelineContract(releaseJar);
        String contractVersion = String.valueOf(contract.get("contractVersion"));
        Path releaseDescriptor = Files.createTempFile("restaurant-pipeline-release-", ".json");
        JSON.writeValue(releaseDescriptor.toFile(), Map.of(
            "schemaVersion", 1,
            "pipelineId", PIPELINE_ID,
            "contractVersion", contractVersion,
            "releaseVersion", contractVersion,
            "artifacts", List.of(Map.of(
                "artifactId", "restaurant-approval-monolith",
                "kind", "jar",
                "uri", releaseJar.toString(),
                "digest", "sha256:" + sha256(releaseJar),
                "stepIds", List.of(),
                "capabilities", List.of("rest-transition-worker")))));
        JsonPath registered = given()
            .baseUri("http://localhost")
            .port(coordinatorPort)
            .header("Authorization", "Bearer " + CONTROL_PLANE_ADMIN_TOKEN)
            .contentType(ContentType.JSON)
            .body(Map.of("releaseDescriptorPath", releaseDescriptor.toString()))
            .when()
            .post("/tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/releases/register", TENANT_ID, PIPELINE_ID)
            .then()
            .statusCode(200)
            .extract()
            .jsonPath();
        String releaseVersion = registered.getString("releaseVersion");
        assertNotNull(releaseVersion, "registered release should expose releaseVersion");

        given()
            .baseUri("http://localhost")
            .port(coordinatorPort)
            .header("Authorization", "Bearer " + CONTROL_PLANE_ADMIN_TOKEN)
            .when()
            .post(
                "/tpf/admin/tenants/{tenantId}/pipelines/{pipelineId}/releases/{releaseVersion}/activate",
                TENANT_ID,
                PIPELINE_ID,
                releaseVersion)
            .then()
            .statusCode(200);
    }

    protected int startApp(String name, Map<String, String> extraProperties) throws Exception {
        Exception last = null;
        for (int attempt = 1; attempt <= 5; attempt++) {
            int port = randomPortCandidate();
            Process process = null;
            try {
                process = startAppProcess(name, port, extraProperties);
                awaitHttp(name, port, "/q/health/live");
                return port;
            } catch (Exception e) {
                last = e;
                if (process != null) {
                    processes.remove(process);
                    stopProcess(process);
                }
            }
        }
        throw new AssertionError("Failed to start " + name + " after retrying port allocation", last);
    }

    private static void assertAcceptedFlow(int coordinatorPort, boolean hosted) throws InterruptedException {
        RunAsyncAcceptedDto accepted = submitOrder(coordinatorPort, "Ada Lovelace", "Cafe TPF", hosted);
        ExecutionStatusDto waiting = awaitExecutionStatus(coordinatorPort, accepted.executionId(), ExecutionStatus.WAITING_EXTERNAL, hosted);
        assertEquals(2, waiting.stepIndex(), "waiting state should point at the await step that suspended execution");

        PendingInteraction pending = awaitPendingInteraction(coordinatorPort, accepted.executionId(), hosted);
        assertEquals(AWAIT_STEP_ID, pending.stepId());
        assertEquals("interaction-api", pending.transportType());

        AwaitCompletionResponseDto completion = completeAccepted(coordinatorPort, pending, hosted);
        assertEquals(pending.interactionId(), completion.interactionId());
        assertEquals(AWAIT_STEP_ID, completion.stepId());
        assertEquals(AwaitInteractionStatus.COMPLETED, completion.status());
        assertFalse(completion.duplicate(), "first completion should not be marked duplicate");

        ExecutionStatusDto succeeded = awaitExecutionStatus(coordinatorPort, accepted.executionId(), ExecutionStatus.SUCCEEDED, hosted);
        assertEquals(3, succeeded.stepIndex(), "execution should resume at the final step after await completion");

        JsonPath result = resultPayload(coordinatorPort, accepted.executionId(), hosted);
        assertEquals(pending.orderId(), result.getString("orderId"));
        assertEquals("APPROVED", result.getString("outcome"));
        assertEquals("ACCEPTED", result.getString("restaurantStatus"));
        assertEquals("Approved by Cafe TPF", result.getString("summary"));
        assertNotNull(result.getString("resolvedAt"));
    }

    private static void assertDeclinedFlow(int coordinatorPort, boolean hosted) throws InterruptedException {
        RunAsyncAcceptedDto accepted = submitOrder(coordinatorPort, "Grace Hopper", "Bistro Queue", hosted);
        awaitExecutionStatus(coordinatorPort, accepted.executionId(), ExecutionStatus.WAITING_EXTERNAL, hosted);

        PendingInteraction pending = awaitPendingInteraction(coordinatorPort, accepted.executionId(), hosted);
        AwaitCompletionResponseDto completion = completeDeclined(coordinatorPort, pending, hosted);
        assertEquals(AwaitInteractionStatus.COMPLETED, completion.status());

        awaitExecutionStatus(coordinatorPort, accepted.executionId(), ExecutionStatus.SUCCEEDED, hosted);

        JsonPath result = resultPayload(coordinatorPort, accepted.executionId(), hosted);
        assertEquals(pending.orderId(), result.getString("orderId"));
        assertEquals("DECLINED", result.getString("outcome"));
        assertEquals("DECLINED", result.getString("restaurantStatus"));
        assertEquals("Need more prep time (Kitchen is overloaded tonight)", result.getString("summary"));
        assertNotNull(result.getString("resolvedAt"));
    }

    private static void stopProcess(Process process) {
        process.destroy();
        try {
            if (!process.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }

    private Process startAppProcess(String name, int port, Map<String, String> extraProperties) throws IOException {
        Path moduleDir = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        Path runner = moduleDir.resolve("target/quarkus-app/quarkus-run.jar");
        assertTrue(Files.isRegularFile(runner), "missing packaged Quarkus app at " + runner);

        Path logsDir = moduleDir.resolve("target/transition-worker-split-it");
        Files.createDirectories(logsDir);
        Path logFile = logsDir.resolve(name + ".log");

        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("--enable-preview");
        command.add("-Dquarkus.http.host=127.0.0.1");
        command.add("-Dquarkus.http.port=" + port);
        command.add("-Dquarkus.http.ssl-port=0");
        command.add("-Dquarkus.http.insecure-requests=enabled");
        command.add("-Dquarkus.grpc.server.use-separate-server=false");
        command.add("-Dquarkus.grpc.server.plain-text=true");
        command.add("-Dpipeline.module.monolith-svc.host=localhost");
        command.add("-Dpipeline.module.monolith-svc.port=" + port);
        command.add("-Dquarkus.rest-client.process-validate-order-request.url=http://localhost:" + port);
        command.add("-Dquarkus.rest-client.process-create-pending-approval.url=http://localhost:" + port);
        command.add("-Dquarkus.rest-client.process-finalize-restaurant-decision.url=http://localhost:" + port);
        command.add("-Dquarkus.otel.enabled=false");
        command.add("-Dquarkus.otel.sdk.disabled=true");
        command.add("-Dquarkus.micrometer.export.prometheus.enabled=false");
        command.add("-Dquarkus.micrometer.binder.http-server.enabled=false");
        command.add("-Dquarkus.micrometer.binder.http-client.enabled=false");
        command.add("-Dquarkus.micrometer.binder.netty.enabled=false");
        extraProperties.forEach((key, value) -> command.add("-D" + key + "=" + value));
        command.add("-jar");
        command.add(runner.toString());

        Process process = new ProcessBuilder(command)
            .directory(moduleDir.toFile())
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.to(logFile.toFile()))
            .start();
        processes.add(process);
        return process;
    }

    private static void awaitHttp(String name, int port, String path) throws Exception {
        HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();
        Instant deadline = Instant.now().plus(STARTUP_TIMEOUT);
        URI uri = URI.create("http://localhost:" + port + path);
        Exception last = null;
        while (Instant.now().isBefore(deadline)) {
            try {
                HttpResponse<Void> response = client.send(
                    HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(2)).GET().build(),
                    HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    return;
                }
            } catch (IOException | InterruptedException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
                last = e;
            }
            Thread.sleep(250L);
        }
        throw new AssertionError("Timed out waiting for " + name + " at " + uri, last);
    }

    private static int randomPortCandidate() {
        return ThreadLocalRandom.current().nextInt(20_000, 60_000);
    }

    private static RunAsyncAcceptedDto submitOrder(int port, String customerName, String restaurantName, boolean hosted) {
        return submitOrderRequest(port, customerName, restaurantName, hosted)
            .statusCode(200)
            .extract()
            .as(RunAsyncAcceptedDto.class);
    }

    private static io.restassured.response.ValidatableResponse submitOrderRequest(
        int port,
        String customerName,
        String restaurantName,
        boolean hosted) {
        String requestId = UUID.randomUUID().toString();
        Map<String, Object> order = Map.of(
            "requestId", requestId,
            "customerName", customerName,
            "restaurantName", restaurantName,
            "items", "Margherita Pizza, Sparkling Water",
            "totalAmount", "27.50",
            "currency", "EUR");
        Object request = hosted
            ? Map.of(
                "pipelineId", PIPELINE_ID,
                "inputShape", ExecutionInputShape.UNI.name(),
                "inputPayload", encodedPayload(order, ORDER_REQUEST_DTO_TYPE),
                "idempotencyKey", "order-" + requestId,
                "outputStreaming", false)
            : order;

        return request(port, hosted)
            .baseUri("http://localhost")
            .port(port)
            .header("Idempotency-Key", "order-" + requestId)
            .contentType(ContentType.JSON)
            .body(request)
            .when()
            .post(hostedPath(hosted, "/executions", "/pipeline/run-async"))
            .then();
    }

    private static ExecutionStatusDto awaitExecutionStatus(int port, String executionId, ExecutionStatus targetStatus, boolean hosted)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(POLL_TIMEOUT);
        ExecutionStatusDto last = null;
        while (Instant.now().isBefore(deadline)) {
            last = request(port, hosted)
                .baseUri("http://localhost")
                .port(port)
                .accept(ContentType.JSON)
                .when()
                .get(hostedPath(hosted, "/executions/{executionId}", "/pipeline/executions/{executionId}"), executionId)
                .then()
                .statusCode(200)
                .extract()
                .as(ExecutionStatusDto.class);
            if (last.status() == targetStatus) {
                return last;
            }
            if (last.status() == ExecutionStatus.FAILED || last.status() == ExecutionStatus.DLQ) {
                throw new AssertionError("execution moved to terminal failure: " + last.status() + " / "
                    + last.errorCode() + " / " + last.errorMessage());
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("execution " + executionId + " did not reach " + targetStatus + "; last status=" + last);
    }

    private static PendingInteraction awaitPendingInteraction(int port, String executionId, boolean hosted) throws InterruptedException {
        Instant deadline = Instant.now().plus(POLL_TIMEOUT);
        while (Instant.now().isBefore(deadline)) {
            List<Map<String, Object>> interactions = request(port, hosted)
                .baseUri("http://localhost")
                .port(port)
                .queryParam("stepId", AWAIT_STEP_ID)
                .accept(ContentType.JSON)
                .when()
                .get(hostedPath(hosted, "/interactions/pending", "/pipeline/interactions/pending"))
                .then()
                .statusCode(200)
                .extract()
                .jsonPath()
                .getList("$");
            for (Map<String, Object> interaction : interactions) {
                if (executionId.equals(interaction.get("executionId"))) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> requestPayload = (Map<String, Object>) interaction.get("requestPayload");
                    return new PendingInteraction(
                        String.valueOf(interaction.get("interactionId")),
                        String.valueOf(interaction.get("stepId")),
                        String.valueOf(interaction.get("transportType")),
                        String.valueOf(requestPayload.get("orderId")));
                }
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("no pending interaction found for execution " + executionId);
    }

    private static JsonPath resultPayload(int port, String executionId, boolean hosted) {
        JsonPath result = request(port, hosted)
            .baseUri("http://localhost")
            .port(port)
            .accept(ContentType.JSON)
            .when()
            .get(hostedPath(hosted, "/executions/{executionId}/result", "/pipeline/executions/{executionId}/result"), executionId)
            .then()
            .statusCode(200)
            .extract()
            .jsonPath();
        if (!hosted) {
            return result;
        }
        String serializedPayload = result.getString("resultPayload.payload");
        return new JsonPath(serializedPayload);
    }

    private static AwaitCompletionResponseDto completeAccepted(int port, PendingInteraction pending, boolean hosted) {
        Map<String, Object> request = Map.of(
            "interactionId", pending.interactionId(),
            "idempotencyKey", "complete-" + pending.interactionId(),
            "actor", "restaurant-demo-ui",
            "responsePayload", Map.of(
                "accepted", Map.of(
                    "orderId", pending.orderId(),
                    "decidedAt", Instant.now().toString(),
                    "note", "Approved by Cafe TPF")));

        return complete(port, pending, request, hosted);
    }

    private static AwaitCompletionResponseDto completeDeclined(int port, PendingInteraction pending, boolean hosted) {
        Map<String, Object> request = Map.of(
            "interactionId", pending.interactionId(),
            "idempotencyKey", "complete-" + pending.interactionId(),
            "actor", "restaurant-demo-ui",
            "responsePayload", Map.of(
                "declined", Map.of(
                    "orderId", pending.orderId(),
                    "decidedAt", Instant.now().toString(),
                    "note", "Need more prep time",
                    "declineReason", "Kitchen is overloaded tonight")));

        return complete(port, pending, request, hosted);
    }

    private static AwaitCompletionResponseDto complete(int port, PendingInteraction pending, Map<String, Object> request, boolean hosted) {
        Object body = hosted
            ? Map.of(
                "interactionId", pending.interactionId(),
                "idempotencyKey", String.valueOf(request.get("idempotencyKey")),
                "responsePayload", encodedPayload(request.get("responsePayload")),
                "actor", String.valueOf(request.get("actor")))
            : request;
        return request(port, hosted)
            .baseUri("http://localhost")
            .port(port)
            .contentType(ContentType.JSON)
            .body(body)
            .when()
            .post(hostedPath(hosted, "/interactions/complete", "/pipeline/interactions/complete"))
            .then()
            .statusCode(200)
            .extract()
            .as(AwaitCompletionResponseDto.class);
    }

    private static RequestSpecification request(int port, boolean hosted) {
        RequestSpecification spec = given().port(port);
        if (hosted) {
            return spec.header("Authorization", "Bearer " + CONTROL_PLANE_ADMIN_TOKEN);
        }
        return spec.header("x-tenant-id", TENANT_ID);
    }

    private static String hostedPath(boolean hosted, String hostedPath, String generatedPath) {
        return hosted ? "/tpf/control-plane/tenants/" + TENANT_ID + hostedPath : generatedPath;
    }

    private static Path locateReleaseArtifact() throws IOException {
        Path moduleDir = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        Path target = moduleDir.resolve("target");
        try (var candidates = Files.walk(target)) {
            List<Path> releaseJars = candidates
                .filter(Files::isRegularFile)
                .filter(path -> path.getFileName().toString().endsWith(".jar"))
                .filter(path -> !path.startsWith(target.resolve("transition-worker-split-it")))
                .filter(path -> !path.startsWith(target.resolve("tpf-self-host")))
                .filter(RestaurantApprovalSplitWorkerITSupport::containsMatchingPipelineContract)
                .sorted(RestaurantApprovalSplitWorkerITSupport::compareReleaseArtifactPreference)
                .toList();
            if (releaseJars.isEmpty()) {
                throw new AssertionError("No built JAR with pipeline contract found under " + target);
            }
            return releaseJars.get(0);
        }
    }

    private static int compareReleaseArtifactPreference(Path left, Path right) {
        return Integer.compare(releaseArtifactPreference(left), releaseArtifactPreference(right));
    }

    private static int releaseArtifactPreference(Path path) {
        Path moduleTarget = Path.of(System.getProperty("user.dir")).toAbsolutePath().resolve("target");
        if (path.startsWith(moduleTarget.resolve("quarkus-app").resolve("app"))) {
            return 0;
        }
        if (path.getParent() != null
            && path.getParent().equals(moduleTarget)
            && path.getFileName().toString().matches("monolith-svc-[^-]+(?:\\.[^-]+)*-SNAPSHOT\\.jar")) {
            return 1;
        }
        return 2;
    }

    private static boolean containsMatchingPipelineContract(Path jarPath) {
        try (JarFile jar = new JarFile(jarPath.toFile())) {
            var entry = jar.getJarEntry("META-INF/pipeline/pipeline-contract.json");
            if (entry == null) {
                return false;
            }
            try (var stream = jar.getInputStream(entry)) {
                return PIPELINE_ID.equals(JSON.readTree(stream).path("pipelineId").asText());
            }
        } catch (Exception ignored) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> readPipelineContract(Path jarPath) throws IOException {
        try (JarFile jar = new JarFile(jarPath.toFile())) {
            var entry = jar.getJarEntry("META-INF/pipeline/pipeline-contract.json");
            if (entry == null) {
                throw new IOException("JAR is missing META-INF/pipeline/pipeline-contract.json");
            }
            try (var stream = jar.getInputStream(entry)) {
                return JSON.readValue(stream, Map.class);
            }
        }
    }

    private static String sha256(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(Files.readAllBytes(path));
            return HexFormat.of().formatHex(digest.digest());
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest algorithm is unavailable", e);
        }
    }

    private static Map<String, Object> encodedPayload(Object payload) {
        return encodedPayload(payload, null);
    }

    private static Map<String, Object> encodedPayload(Object payload, String payloadTypeId) {
        try {
            if (payload == null) {
                return Map.of(
                    "payloadTypeId", "null",
                    "payloadEncoding", PAYLOAD_ENCODING,
                    "payload", "null");
            }
            if (payloadTypeId != null && !payloadTypeId.isBlank()) {
                return Map.of(
                    "payloadTypeId", payloadTypeId,
                    "payloadEncoding", PAYLOAD_ENCODING,
                    "payload", JSON.writeValueAsString(payload));
            }
            if (payload instanceof Map<?, ?> map) {
                Map<String, Object> items = new java.util.LinkedHashMap<>();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    items.put(String.valueOf(entry.getKey()), encodedPayload(entry.getValue()));
                }
                return Map.of(
                    "payloadTypeId", "java.util.Map",
                    "payloadEncoding", PAYLOAD_ENCODING,
                    "payload", JSON.writeValueAsString(Map.of("items", items)));
            }
            if (payload instanceof Iterable<?> iterable) {
                List<Object> items = new ArrayList<>();
                iterable.forEach(item -> items.add(encodedPayload(item)));
                return Map.of(
                    "payloadTypeId", "java.util.List",
                    "payloadEncoding", PAYLOAD_ENCODING,
                    "payload", JSON.writeValueAsString(Map.of("items", items)));
            }
            return Map.of(
                "payloadTypeId", payload.getClass().getName(),
                "payloadEncoding", PAYLOAD_ENCODING,
                "payload", JSON.writeValueAsString(payload));
        } catch (IOException e) {
            throw new IllegalStateException("Failed encoding hosted control-plane test payload", e);
        }
    }

    private record PendingInteraction(String interactionId, String stepId, String transportType, String orderId) {
    }
}
