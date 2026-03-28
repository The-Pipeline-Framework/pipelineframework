package org.pipelineframework.tpfgo.e2e;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.checkpoint.CheckpointPublicationProtoSupport;
import org.pipelineframework.checkpoint.CheckpointPublicationRequest;
import org.pipelineframework.checkpoint.grpc.CheckpointPublishAcceptedResponse;
import org.pipelineframework.checkpoint.grpc.CheckpointPublishRequest;
import org.pipelineframework.checkpoint.grpc.CheckpointPublicationServiceGrpc;
import org.pipelineframework.checkpoint.grpc.MutinyCheckpointPublicationServiceGrpc;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.tpfgo.checkout.grpc.CheckoutValidateRequestSvc;
import org.pipelineframework.tpfgo.checkout.grpc.Orchestrator;
import org.pipelineframework.tpfgo.checkout.grpc.OrchestratorServiceGrpc;
import org.pipelineframework.tpfgo.common.util.DeterministicIds;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TpfgoCheckpointFlowIT {

    private static final HttpClient HTTP = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(2))
        .build();

    private final List<ManagedApp> apps = new ArrayList<>();
    private FinalCollector collector;
    private Path logDirectory;

    @BeforeEach
    void setUp() throws Exception {
        logDirectory = Path.of("target", "failsafe-reports", "tpfgo-checkpoint-flow");
        Files.createDirectories(logDirectory);
        collector = new FinalCollector(findFreePort());
        collector.start();

        List<AppSpec> specs = specs(collector.port());
        for (AppSpec spec : specs) {
            apps.add(ManagedApp.start(spec, logDirectory));
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        for (ManagedApp app : apps.reversed()) {
            app.close();
        }
        apps.clear();
        if (collector != null) {
            collector.close();
            collector = null;
        }
    }

    @Test
    void executesFullCanonicalTpfgoFlowOverGrpcCheckpointHandoff() {
        ManagedApp checkout = app("checkout-orchestrator-svc");
        Orchestrator.RunAsyncResponse accepted = checkout.orchestrator().runAsync(
            Orchestrator.RunAsyncRequest.newBuilder()
                .setInput(CheckoutValidateRequestSvc.PlaceOrderRequest.newBuilder()
                    .setRequestId("11111111-1111-1111-1111-111111111111")
                    .setCustomerId("22222222-2222-2222-2222-222222222222")
                    .setRestaurantId("33333333-3333-3333-3333-333333333333")
                    .setItems("burger x1,fries x1,soda x1")
                    .setTotalAmount("42.50")
                    .setCurrency("EUR")
                    .build())
                .setTenantId("default")
                .setIdempotencyKey("tpfgo-happy-1")
                .build());

        assertFalse(accepted.getDuplicate());
        JsonNode finalPayload = collector.awaitPayload("tpfgo.compensation.terminal-state.v1");
        assertEquals("COMPLETED", finalPayload.get("outcome").asText());
        assertEquals("CAPTURED", finalPayload.get("paymentStatus").asText());
        assertEquals("none", finalPayload.get("resolutionAction").asText());
        assertEquals(
            DeterministicIds.uuid(
                "order",
                "11111111-1111-1111-1111-111111111111",
                "22222222-2222-2222-2222-222222222222",
                "33333333-3333-3333-3333-333333333333").toString(),
            finalPayload.get("orderId").asText());
    }

    @Test
    void routesPaymentFailureIntoCompensationTerminalState() {
        ManagedApp checkout = app("checkout-orchestrator-svc");
        checkout.orchestrator().runAsync(
            Orchestrator.RunAsyncRequest.newBuilder()
                .setInput(CheckoutValidateRequestSvc.PlaceOrderRequest.newBuilder()
                    .setRequestId("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                    .setCustomerId("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
                    .setRestaurantId("cccccccc-cccc-cccc-cccc-cccccccccccc")
                    .setItems("burger x1")
                    .setTotalAmount("0")
                    .setCurrency("EUR")
                    .build())
                .setTenantId("default")
                .setIdempotencyKey("tpfgo-failure-1")
                .build());

        JsonNode finalPayload = collector.awaitPayload("tpfgo.compensation.terminal-state.v1");
        assertEquals("FAILED_COMPENSATED", finalPayload.get("outcome").asText());
        assertEquals("FAILED", finalPayload.get("paymentStatus").asText());
        assertEquals("PAYMENT_CAPTURE_REJECTED", finalPayload.get("failureCode").asText());
        assertEquals("manual-review", finalPayload.get("resolutionAction").asText());
    }

    @Test
    void deduplicatesRepeatedCheckpointAdmissionAtCompensationBoundary() throws Exception {
        ManagedApp compensation = app("compensation-failure-orchestrator-svc");
        ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", compensation.grpcPort())
            .usePlaintext()
            .build();
        try {
            CheckpointPublicationServiceGrpc.CheckpointPublicationServiceBlockingStub stub =
                CheckpointPublicationServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(10, TimeUnit.SECONDS);

            JsonNode payload = PipelineJson.mapper().valueToTree(Map.of(
                "orderId", "dddddddd-dddd-dddd-dddd-dddddddddddd",
                "paymentId", "",
                "processedAt", "2026-03-27T12:00:00Z",
                "amount", "0",
                "currency", "EUR",
                "status", "FAILED",
                "failureCode", "PAYMENT_CAPTURE_REJECTED",
                "failureReason", "amount must be positive"));

            CheckpointPublishRequest request = CheckpointPublishRequest.newBuilder()
                .setPublication("tpfgo.payment.capture-result.v1")
                .setPayloadJson(ByteString.copyFrom(PipelineJson.mapper().writeValueAsBytes(payload)))
                .setTenantId("default")
                .setIdempotencyKey("payment-boundary-duplicate")
                .build();

            CheckpointPublishAcceptedResponse first = stub.publish(request);
            CheckpointPublishAcceptedResponse duplicate = stub.publish(request);

            assertFalse(first.getDuplicate());
            assertTrue(duplicate.getDuplicate());
            JsonNode finalPayload = collector.awaitPayload("tpfgo.compensation.terminal-state.v1");
            assertEquals("FAILED_COMPENSATED", finalPayload.get("outcome").asText());
            assertEquals(1, collector.countFor("tpfgo.compensation.terminal-state.v1"));
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private ManagedApp app(String moduleDir) {
        return apps.stream()
            .filter(app -> app.moduleDir().equals(moduleDir))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Unknown app " + moduleDir));
    }

    private List<AppSpec> specs(int collectorPort) throws IOException {
        AppSpec pipelineRuntime = runtimeSpec("pipeline-runtime-svc");
        int internalGrpcPort = pipelineRuntime.grpcPort();

        AppSpec checkout = orchestratorSpec(
            "checkout-orchestrator-svc",
            "tpfgo.checkout.order-pending.v1",
            internalGrpcPort,
            List.of("process-checkout-validate-request", "process-checkout-create-pending"));
        AppSpec consumer = orchestratorSpec(
            "consumer-validation-orchestrator-svc",
            "tpfgo.consumer.order-approved.v1",
            internalGrpcPort,
            List.of("process-consumer-validate-order"));
        AppSpec restaurant = orchestratorSpec(
            "restaurant-acceptance-orchestrator-svc",
            "tpfgo.restaurant.order-accepted.v1",
            internalGrpcPort,
            List.of("process-restaurant-accept-order"));
        AppSpec kitchen = orchestratorSpec(
            "kitchen-preparation-orchestrator-svc",
            "tpfgo.kitchen.order-ready.v1",
            internalGrpcPort,
            List.of("process-kitchen-expand-tasks", "process-kitchen-reduce-completion"));
        AppSpec dispatch = orchestratorSpec(
            "dispatch-orchestrator-svc",
            "tpfgo.dispatch.delivery-assigned.v1",
            internalGrpcPort,
            List.of("process-dispatch-assign-courier"));
        AppSpec delivery = orchestratorSpec(
            "delivery-execution-orchestrator-svc",
            "tpfgo.delivery.order-delivered.v1",
            internalGrpcPort,
            List.of("process-delivery-execute-order"));
        AppSpec payment = orchestratorSpec(
            "payment-capture-orchestrator-svc",
            "tpfgo.payment.capture-result.v1",
            internalGrpcPort,
            List.of("process-payment-capture-order"));
        AppSpec compensation = orchestratorSpec(
            "compensation-failure-orchestrator-svc",
            "tpfgo.compensation.terminal-state.v1",
            internalGrpcPort,
            List.of("process-compensation-finalize-order"));

        checkout.bindTo(consumer.grpcPort());
        consumer.bindTo(restaurant.grpcPort());
        restaurant.bindTo(kitchen.grpcPort());
        kitchen.bindTo(dispatch.grpcPort());
        dispatch.bindTo(delivery.grpcPort());
        delivery.bindTo(payment.grpcPort());
        payment.bindTo(compensation.grpcPort());
        compensation.bindTo(collectorPort);

        return List.of(
            pipelineRuntime,
            checkout,
            consumer,
            restaurant,
            kitchen,
            dispatch,
            delivery,
            payment,
            compensation);
    }

    private AppSpec runtimeSpec(String moduleDir) throws IOException {
        int httpPort = findFreePort();
        int grpcPort = httpPort;
        return new AppSpec(moduleDir, httpPort, grpcPort, false, null, 0, List.of(), new ArrayList<>());
    }

    private AppSpec orchestratorSpec(
        String moduleDir,
        String publication,
        int internalGrpcTargetPort,
        List<String> internalGrpcClients
    ) throws IOException {
        int httpPort = findFreePort();
        int grpcPort = findFreePort();
        return new AppSpec(
            moduleDir,
            httpPort,
            grpcPort,
            true,
            publication,
            internalGrpcTargetPort,
            internalGrpcClients,
            new ArrayList<>());
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket()) {
            socket.bind(new InetSocketAddress("127.0.0.1", 0));
            return socket.getLocalPort();
        }
    }

    private record AppSpec(
        String moduleDir,
        int httpPort,
        int grpcPort,
        boolean orchestrator,
        String publication,
        int internalGrpcTargetPort,
        List<String> internalGrpcClients,
        List<String> bindingLines
    ) {
        private void bindTo(int targetGrpcPort) {
            String bindingPrefix = "pipeline.handoff.bindings.\"" + publication + "\".targets.next";
            bindingLines.add(bindingPrefix + ".kind=GRPC");
            bindingLines.add(bindingPrefix + ".host=127.0.0.1");
            bindingLines.add(bindingPrefix + ".port=" + targetGrpcPort);
            bindingLines.add(bindingPrefix + ".plaintext=true");
        }

        private List<String> internalGrpcClientLines() {
            List<String> lines = new ArrayList<>();
            for (String client : internalGrpcClients) {
                lines.add("quarkus.grpc.clients." + client + ".host=127.0.0.1");
                lines.add("quarkus.grpc.clients." + client + ".port=" + internalGrpcTargetPort);
                lines.add("quarkus.grpc.clients." + client + ".plain-text=true");
            }
            return lines;
        }
    }

    private static final class ManagedApp implements AutoCloseable {

        private final String moduleDir;
        private final int grpcPort;
        private final Process process;
        private final ManagedChannel channel;

        private ManagedApp(String moduleDir, int grpcPort, Process process, ManagedChannel channel) {
            this.moduleDir = moduleDir;
            this.grpcPort = grpcPort;
            this.process = process;
            this.channel = channel;
        }

        static ManagedApp start(AppSpec spec, Path logDirectory) throws Exception {
            Path moduleRoot = resolveModuleRoot(spec.moduleDir());
            Path configFile = logDirectory.resolve(spec.moduleDir() + ".properties");
            List<String> lines = new ArrayList<>();
            lines.add("quarkus.http.host=127.0.0.1");
            lines.add("quarkus.http.port=" + spec.httpPort());
            lines.add("quarkus.grpc.server.host=127.0.0.1");
            lines.add("quarkus.grpc.server.port=" + spec.grpcPort());
            lines.add("quarkus.otel.sdk.disabled=true");
            if (spec.orchestrator()) {
                lines.add("pipeline.orchestrator.mode=QUEUE_ASYNC");
                lines.add("pipeline.orchestrator.idempotency-policy=CLIENT_KEY_REQUIRED");
                lines.add("pipeline.orchestrator.state-provider=memory");
                lines.add("pipeline.orchestrator.dispatcher-provider=event");
                lines.add("pipeline.orchestrator.dlq-provider=log");
            }
            lines.addAll(spec.internalGrpcClientLines());
            lines.addAll(spec.bindingLines());
            Files.writeString(configFile, String.join(System.lineSeparator(), lines) + System.lineSeparator());

            Path logFile = logDirectory.resolve(spec.moduleDir() + ".log");
            ProcessBuilder builder = new ProcessBuilder(
                "java",
                "-jar",
                "target/quarkus-app/quarkus-run.jar");
            builder.directory(moduleRoot.toFile());
            builder.redirectErrorStream(true);
            builder.redirectOutput(logFile.toFile());
            builder.environment().put("QUARKUS_CONFIG_LOCATIONS", configFile.toAbsolutePath().toString());
            Process process = builder.start();
            try {
                waitForHealth(spec.httpPort(), process);
            } catch (Exception e) {
                destroyProcess(process);
                String startupLog = Files.exists(logFile) ? Files.readString(logFile) : "";
                throw new IllegalStateException(
                    "Failed to start app '" + spec.moduleDir() + "' on HTTP port " + spec.httpPort()
                        + " and gRPC port " + spec.grpcPort() + ". Log:\n" + startupLog,
                    e);
            }

            ManagedChannel channel = null;
            if (spec.orchestrator()) {
                channel = ManagedChannelBuilder.forAddress("127.0.0.1", spec.grpcPort())
                    .usePlaintext()
                    .build();
            }
            return new ManagedApp(spec.moduleDir(), spec.grpcPort(), process, channel);
        }

        private static Path resolveModuleRoot(String moduleDir) {
            Path cwd = Path.of("").toAbsolutePath().normalize();
            for (Path cursor = cwd; cursor != null; cursor = cursor.getParent()) {
                Path directCandidate = cursor.resolve(moduleDir);
                if (Files.isDirectory(directCandidate.resolve("src"))
                    && Files.exists(directCandidate.resolve("pom.xml"))) {
                    return directCandidate;
                }

                Path checkoutCandidate = cursor.resolve("examples").resolve("checkout").resolve(moduleDir);
                if (Files.isDirectory(checkoutCandidate.resolve("src"))
                    && Files.exists(checkoutCandidate.resolve("pom.xml"))) {
                    return checkoutCandidate;
                }
            }
            throw new IllegalStateException("Could not resolve module root for " + moduleDir);
        }

        private static void destroyProcess(Process process) throws InterruptedException {
            if (!process.isAlive()) {
                return;
            }
            process.destroy();
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }

        private static void waitForHealth(int port, Process process) {
            Awaitility.await()
                .atMost(Duration.ofSeconds(40))
                .pollInterval(Duration.ofMillis(250))
                .until(() -> healthReady(port, process));
        }

        private static boolean healthReady(int port, Process process) {
            if (!process.isAlive()) {
                throw new IllegalStateException("Application process exited before becoming healthy");
            }
            try {
                HttpResponse<String> response = HTTP.send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/q/health"))
                        .timeout(Duration.ofSeconds(2))
                        .GET()
                        .build(),
                    HttpResponse.BodyHandlers.ofString());
                return response.statusCode() == 200;
            } catch (IOException e) {
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for health endpoint", e);
            }
        }

        String moduleDir() {
            return moduleDir;
        }

        int grpcPort() {
            return grpcPort;
        }

        OrchestratorServiceGrpc.OrchestratorServiceBlockingStub orchestrator() {
            if (channel == null) {
                throw new IllegalStateException("No orchestrator client available for module " + moduleDir);
            }
            return OrchestratorServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(10, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            if (channel != null) {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
            if (process.isAlive()) {
                process.destroy();
                if (!process.waitFor(10, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    process.waitFor(10, TimeUnit.SECONDS);
                }
            }
        }
    }

    private static final class FinalCollector extends MutinyCheckpointPublicationServiceGrpc.CheckpointPublicationServiceImplBase
        implements AutoCloseable {

        private final int port;
        private final List<CheckpointPublishRequest> received = new CopyOnWriteArrayList<>();
        private final Server server;

        private FinalCollector(int port) throws IOException {
            this.port = port;
            this.server = ServerBuilder.forPort(port).addService(this).build();
        }

        int port() {
            return port;
        }

        void start() throws IOException {
            server.start();
        }

        JsonNode awaitPayload(String publication) {
            Awaitility.await()
                .atMost(Duration.ofSeconds(40))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> countFor(publication), count -> count > 0);
            return received.stream()
                .filter(request -> Objects.equals(publication, request.getPublication()))
                .reduce((first, second) -> second)
                .map(this::toPayload)
                .orElseThrow();
        }

        int countFor(String publication) {
            return (int) received.stream()
                .filter(request -> Objects.equals(publication, request.getPublication()))
                .count();
        }

        @Override
        public io.smallrye.mutiny.Uni<CheckpointPublishAcceptedResponse> publish(CheckpointPublishRequest request) {
            received.add(request);
            return io.smallrye.mutiny.Uni.createFrom().item(
                CheckpointPublishAcceptedResponse.newBuilder()
                    .setExecutionId("collector-" + received.size())
                    .setStatusUrl("/collector/" + received.size())
                    .setSubmittedAtEpochMs(System.currentTimeMillis())
                    .build());
        }

        private JsonNode toPayload(CheckpointPublishRequest request) {
            try {
                return CheckpointPublicationProtoSupport.fromProtoRequest(request).payload();
            } catch (IOException e) {
                throw new IllegalStateException("Failed to decode collected checkpoint payload", e);
            }
        }

        @Override
        public void close() throws Exception {
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
