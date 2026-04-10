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

    /**
     * Prepares the integration test environment by creating the per-test log directory, starting
     * the in-process FinalCollector on a free port, and launching ManagedApp processes for all
     * configured AppSpec instances.
     *
     * @throws Exception if creating directories, starting the collector, or launching any managed
     *                   application fails
     */
    @BeforeEach
    void setUp() throws Exception {
        logDirectory = Path.of("target", "failsafe-reports", "tpfgo-checkpoint-flow");
        Files.createDirectories(logDirectory);
        ServerSocket collectorSocket = reservePort();
        try {
            int collectorPort = collectorSocket.getLocalPort();
            collectorSocket.close();
            collector = new FinalCollector(collectorPort);
            collector.start();

            List<AppSpec> specs = specs(collector.port());
            for (AppSpec spec : specs) {
                apps.add(ManagedApp.start(spec, logDirectory));
            }
        } catch (Exception e) {
            collectorSocket.close();
            throw e;
        }
    }

    /**
     * Cleans up resources created for a test by closing all started managed applications and the final collector.
     *
     * Closes managed applications in reverse startup order, clears the apps list, and closes the collector if present.
     *
     * @throws Exception if shutting down any managed application or the collector fails
     */
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

    /**
     * Executes the canonical end-to-end TPFGO order flow and verifies the compensation terminal-state published via gRPC checkpoint handoff.
     *
     * Submits a fixed PlaceOrder request to the checkout orchestrator, asserts the submission is not a duplicate, waits for the terminal-state checkpoint published to
     * "tpfgo.compensation.terminal-state.v1", and verifies the terminal payload indicates a completed outcome with captured payment, no resolution action, and the expected deterministic orderId.
     */
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

    /**
     * Verifies that submitting an order with a zero total amount results in a compensated terminal-state publication.
     *
     * Sends a PlaceOrder request with `totalAmount="0"` to the checkout orchestrator, waits for the
     * `tpfgo.compensation.terminal-state.v1` payload from the collector, and asserts that the terminal
     * payload indicates a compensated failure with the expected payment status, failure code, and resolution action.
     */
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

    /**
     * Locate a started ManagedApp by its module directory identifier.
     *
     * @param moduleDir the module directory identifier to match
     * @return the matching ManagedApp
     * @throws IllegalStateException if no app with the given moduleDir is found
     */
    private ManagedApp app(String moduleDir) {
        return apps.stream()
            .filter(app -> app.moduleDir().equals(moduleDir))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("Unknown app " + moduleDir));
    }

    /**
     * Builds AppSpec instances for the pipeline runtime and all orchestrator modules, configures
     * the sequential handoff bindings between them, and binds the final orchestrator to the
     * provided collector port.
     *
     * @param collectorPort the port on which the in-process FinalCollector is listening; the final
     *                      orchestrator will publish to this port
     * @return a list of AppSpec values in the order they should be started (pipeline runtime first,
     *         followed by orchestrators with their binding lines already configured)
     * @throws IOException if a required free port cannot be allocated or underlying spec construction fails
     */
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

    /**
     * Create an AppSpec for a non-orchestrator runtime module with a free HTTP/gRPC port.
     *
     * @param moduleDir the module directory/name used to identify the module
     * @return an AppSpec configured for a runtime module (not an orchestrator) with its HTTP and gRPC ports set to an available port and no publications or internal clients
     * @throws IOException if a free port cannot be allocated
     */
    private AppSpec runtimeSpec(String moduleDir) throws IOException {
        ServerSocket socket = reservePort();
        try {
            int httpPort = socket.getLocalPort();
            int grpcPort = httpPort;
            return new AppSpec(moduleDir, httpPort, grpcPort, false, null, 0, List.of(), new ArrayList<>());
        } finally {
            socket.close();
        }
    }

    /**
     * Create an AppSpec configured as an orchestrator with freshly allocated HTTP and gRPC ports.
     *
     * @param moduleDir               module directory identifier used to resolve and start the module
     * @param publication             the handoff publication name this orchestrator will emit
     * @param internalGrpcTargetPort  target port to configure internal gRPC clients to connect to
     * @param internalGrpcClients     names of internal gRPC client bindings to generate
     * @return                        an AppSpec populated for an orchestrator, with free HTTP/grpc ports and empty binding lines
     * @throws IOException            if a free port cannot be acquired for HTTP or gRPC
     */
    private AppSpec orchestratorSpec(
        String moduleDir,
        String publication,
        int internalGrpcTargetPort,
        List<String> internalGrpcClients
    ) throws IOException {
        ServerSocket httpSocket = reservePort();
        ServerSocket grpcSocket = reservePort();
        try {
            int httpPort = httpSocket.getLocalPort();
            int grpcPort = grpcSocket.getLocalPort();
            return new AppSpec(
                moduleDir,
                httpPort,
                grpcPort,
                true,
                publication,
                internalGrpcTargetPort,
                internalGrpcClients,
                new ArrayList<>());
        } finally {
            httpSocket.close();
            grpcSocket.close();
        }
    }

    /**
     * Selects an available TCP port bound to the loopback address (127.0.0.1).
     *
     * @return the selected free port number
     * @throws IOException if an I/O error occurs while opening or binding the socket
     * @deprecated Use reservePort() instead to avoid TOCTOU race conditions
     */
    @Deprecated
    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket()) {
            socket.bind(new InetSocketAddress("127.0.0.1", 0));
            return socket.getLocalPort();
        }
    }

    /**
     * Reserves an available TCP port by binding a ServerSocket that remains open.
     * The caller must close the returned socket after the spawned process has bound the port.
     *
     * @return a ServerSocket bound to 127.0.0.1 on an available port
     * @throws IOException if an I/O error occurs while opening or binding the socket
     */
    private static ServerSocket reservePort() throws IOException {
        ServerSocket socket = new ServerSocket();
        socket.bind(new InetSocketAddress("127.0.0.1", 0));
        socket.setReuseAddress(true);
        return socket;
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
        /**
         * Adds configuration property lines that bind this AppSpec's publication to a gRPC target at the given port.
         *
         * @param targetGrpcPort the target gRPC port to which the publication should be forwarded
         */
        private void bindTo(int targetGrpcPort) {
            String bindingPrefix = "pipeline.handoff.bindings.\"" + publication + "\".targets.next";
            bindingLines.add(bindingPrefix + ".kind=GRPC");
            bindingLines.add(bindingPrefix + ".host=127.0.0.1");
            bindingLines.add(bindingPrefix + ".port=" + targetGrpcPort);
            bindingLines.add(bindingPrefix + ".plaintext=true");
        }

        /**
         * Generate Quarkus gRPC client property lines for the module's internal gRPC clients.
         *
         * @return a list of property lines where each internal client contributes three entries:
         *         `quarkus.grpc.clients.<name>.host=127.0.0.1`,
         *         `quarkus.grpc.clients.<name>.port=<internalGrpcTargetPort>`,
         *         and `quarkus.grpc.clients.<name>.plain-text=true`.
         */
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

        /**
         * Creates a ManagedApp representing a started module process, its gRPC port, and an optional orchestrator channel.
         *
         * @param moduleDir identifier of the module used for config/log naming and locating the module root
         * @param grpcPort gRPC server port exposed by the module
         * @param process spawned Java process running the module
         * @param channel plaintext gRPC ManagedChannel to the module's orchestrator endpoint, or `null` for non-orchestrator modules
         */
        private ManagedApp(String moduleDir, int grpcPort, Process process, ManagedChannel channel) {
            this.moduleDir = moduleDir;
            this.grpcPort = grpcPort;
            this.process = process;
            this.channel = channel;
        }

        /**
         * Start a module JVM running Quarkus with a generated per-module properties file, wait until its health endpoint is ready,
         * and return a ManagedApp representing the running process and optional orchestrator gRPC channel.
         *
         * @param spec configuration describing the module to start (ports, orchestrator flag, and binding/client lines)
         * @param logDirectory directory where the generated properties file and module log will be written
         * @return a ManagedApp containing the module directory name, exposed gRPC port, spawned Process, and a ManagedChannel when the module is an orchestrator (null otherwise)
         * @throws Exception if the process cannot be started or the module fails to become healthy within the startup timeout
         */
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

        /**
         * Locate the filesystem root directory for the given module name by searching upward
         * from the current working directory and checking common repository locations.
         *
         * @param moduleDir the module directory name to find (e.g., "checkout-orchestrator-svc")
         * @return the Path to the module's root directory
         * @throws IllegalStateException if no suitable module root is found
         */
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

        /**
         * Attempts to terminate the given process: requests a graceful shutdown and, if the process
         * does not exit within 5 seconds, forces termination and waits up to another 5 seconds.
         *
         * @param process the process to terminate
         * @throws InterruptedException if the current thread is interrupted while waiting for the process to exit
         */
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

        /**
         * Waits until the given process reports healthy by polling its HTTP health endpoint.
         *
         * @param port    HTTP port used for the health check (requests made to 127.0.0.1:port)
         * @param process the process being monitored; if the process exits before becoming healthy this method will fail
         * @throws IllegalStateException if the process terminates before a successful health check or the thread is interrupted
         * @throws org.awaitility.core.ConditionTimeoutException if the health check does not succeed within 40 seconds
         */
        private static void waitForHealth(int port, Process process) {
            Awaitility.await()
                .atMost(Duration.ofSeconds(40))
                .pollInterval(Duration.ofMillis(250))
                .until(() -> healthReady(port, process));
        }

        /**
         * Checks whether the given process is still running and its /q/health endpoint responds with HTTP 200.
         *
         * @param port    the HTTP port where the process exposes its health endpoint
         * @param process the process being monitored
         * @return        `true` if the health endpoint returned status 200, `false` otherwise
         * @throws IllegalStateException if the process has exited before becoming healthy or if the thread is interrupted while waiting
         */
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

        /**
         * The module directory identifier used to locate the module root and generate per-module configuration.
         *
         * @return the module directory name for this specification
         */
        String moduleDir() {
            return moduleDir;
        }

        /**
         * Get the gRPC server port exposed by the module.
         *
         * @return the gRPC port number used by the module
         */
        int grpcPort() {
            return grpcPort;
        }

        /**
         * Obtain a blocking gRPC stub for the orchestrator service configured with a 10-second deadline.
         *
         * @return a blocking stub for the Orchestrator gRPC service
         * @throws IllegalStateException if no orchestrator gRPC channel is available for this module
         */
        OrchestratorServiceGrpc.OrchestratorServiceBlockingStub orchestrator() {
            if (channel == null) {
                throw new IllegalStateException("No orchestrator client available for module " + moduleDir);
            }
            return OrchestratorServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(10, TimeUnit.SECONDS);
        }

        /**
         * Shuts down the managed resources: closes the gRPC channel if present and stops the spawned process.
         *
         * Attempts to terminate the gRPC channel immediately and waits up to 5 seconds for it to finish.
         * If the process is still alive, attempts a graceful shutdown and waits up to 10 seconds, then
         * forces termination and waits up to another 10 seconds.
         *
         * @throws Exception if the current thread is interrupted while waiting for termination
         */
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

        /**
         * Construct a FinalCollector and build a gRPC server bound to the given port.
         *
         * @param port the gRPC port to listen on
         * @throws IOException if the server cannot be created or bound to the port
         */
        private FinalCollector(int port) throws IOException {
            this.port = port;
            this.server = ServerBuilder.forPort(port).addService(this).build();
        }

        /**
         * The gRPC port the collector is listening on.
         *
         * @return the port number
         */
        int port() {
            return port;
        }

        /**
         * Starts the internal gRPC server so it begins listening for checkpoint publish requests.
         *
         * @throws IOException if the server fails to start or bind to the configured port
         */
        void start() throws IOException {
            server.start();
        }

        /**
         * Waits until a checkpoint with the given publication name is received and returns its decoded payload.
         *
         * @param publication the checkpoint publication name to wait for
         * @return the decoded JSON payload of the most recently received checkpoint with the specified publication name
         */
        JsonNode awaitPayload(String publication) {
            Awaitility.await()
                .atMost(Duration.ofSeconds(40))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> countFor(publication), count -> count > 0);
            return received.stream()
                .filter(request -> Objects.equals(publication, request.getPublication()))
                .reduce((ignored, latest) -> latest)
                .map(this::toPayload)
                .orElseThrow();
        }

        /**
         * Count received checkpoint publish requests that have the given publication name.
         *
         * @param publication the publication name to match
         * @return the number of received requests whose publication equals the provided name
         */
        int countFor(String publication) {
            return (int) received.stream()
                .filter(request -> Objects.equals(publication, request.getPublication()))
                .count();
        }

        /**
         * Records an incoming checkpoint publication and returns an acceptance response.
         *
         * @param request the incoming checkpoint publication request to record
         * @return a CheckpointPublishAcceptedResponse containing:
         *         - executionId set to "collector-N" where N is the count after recording,
         *         - statusUrl set to "/collector/N",
         *         - submittedAtEpochMs set to the current epoch millisecond timestamp
         */
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

        /**
         * Decode a checkpoint publish request into its JSON payload.
         *
         * @param request the checkpoint publish request to decode
         * @return the decoded payload as a JsonNode
         * @throws IllegalStateException if the request cannot be decoded into JSON
         */
        private JsonNode toPayload(CheckpointPublishRequest request) {
            try {
                return CheckpointPublicationProtoSupport.fromProtoRequest(request).payload();
            } catch (IOException e) {
                throw new IllegalStateException("Failed to decode collected checkpoint payload", e);
            }
        }

        /**
         * Shuts down the gRPC server immediately and waits up to 5 seconds for it to terminate.
         *
         * @throws InterruptedException if the current thread is interrupted while waiting for termination
         */
        @Override
        public void close() throws Exception {
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}