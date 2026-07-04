/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.csv.orchestrator.service;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.telemetry.PipelineExecutionEvent;
import org.pipelineframework.telemetry.PipelineReplayDocument;
import org.pipelineframework.telemetry.PipelineReplayRunParameters;
import org.pipelineframework.telemetry.PipelineTelemetry;
import org.pipelineframework.telemetry.PipelineReplayTopology;

abstract class AbstractCsvPaymentsEndToEnd {

    private static final Logger LOG = Logger.getLogger(AbstractCsvPaymentsEndToEnd.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String AWAIT_INTERACTION_DISPATCHED = "await_interaction_dispatched";
    private static final String AWAIT_UNIT_DISPATCH_COMPLETE = "await_unit_dispatch_complete";
    private static final String AWAIT_EXECUTION_WAITING = "await_execution_waiting";
    private static final String AWAIT_UNIT_ITEM_COMPLETED = "await_unit_item_completed";
    private static final String AWAIT_UNIT_COMPLETED = "await_unit_completed";
    private static final String AWAIT_RESUME_RELEASED = "await_resume_released";

    private static final Network network = Network.newNetwork();
    private static final String TEST_E2E_DIR = System.getProperty("user.dir") + "/target/test-e2e";
    private static final String TEST_E2E_TARGET_DIR = "/app/test-e2e";
    private static final Path REPLAY_ROOT_DIR = Paths.get(TEST_E2E_DIR, "replay");
    private static final Path REPLAY_CAPTURE_DIR = REPLAY_ROOT_DIR.resolve("csv-payments-runs");
    private static final Path REPLAY_FILE = REPLAY_ROOT_DIR.resolve("csv-payments-replay.json");
    private static final String REPLAY_CAPTURE_CONTAINER_DIR = TEST_E2E_TARGET_DIR + "/replay/csv-payments-runs";
    private static final String LGTM_IMAGE = "docker.io/grafana/otel-lgtm:0.24.0";
    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("apache/kafka-native:3.8.0");
    private static final String KAFKA_NETWORK_ALIAS = "kafka";
    private static final String KAFKA_NETWORK_BOOTSTRAP = KAFKA_NETWORK_ALIAS + ":19092";
    private static final String PAYMENT_REQUEST_TOPIC = "csv-payments.payment.requests";
    private static final String PAYMENT_RESULT_TOPIC = "csv-payments.payment.results";
    private static final String AWAIT_RESPONSES_GROUP_ENV = "TPF_AWAIT_KAFKA_RESPONSES_GROUP_ID";
    private static final String AWAIT_RESPONSES_OFFSET_RESET_ENV = "TPF_AWAIT_KAFKA_RESPONSES_OFFSET_RESET";
    private static final String PROVIDER_REQUESTS_GROUP_ENV = "CSV_PAYMENT_PROVIDER_REQUESTS_GROUP_ID";
    private static final String PROVIDER_REQUESTS_OFFSET_RESET_ENV = "CSV_PAYMENT_PROVIDER_REQUESTS_OFFSET_RESET";
    private static final String E2E_RESUME_TOKEN_SECRET = "csv-payments-e2e-resume-token-secret";
    private static final Duration LGTM_STARTUP_TIMEOUT = Duration.ofMinutes(3);
    private static final Duration TEMPO_HTTP_REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final long TEMPO_SEARCH_TIMEOUT_SECONDS = 90L;
    private static final long TEMPO_SEARCH_POLL_MILLIS = 2_000L;
    private static final long TEMPO_LOCAL_PAUSE_SECONDS = 600L;
    private static final HttpClient HTTP_CLIENT =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private static final Path DEV_CERTS_DIR =
            Paths.get(System.getProperty("user.dir"))
                    .resolve("../target/dev-certs")
                    .normalize()
                    .toAbsolutePath();
    private static final String CONTAINER_KEYSTORE_PATH = "/deployments/server-keystore.jks";
    private static final String CONTAINER_TRUSTSTORE_PATH = "/deployments/client-truststore.jks";
    private static final String RUNTIME_LAYOUT =
            firstNonBlank(System.getProperty("csv.runtime.layout"), System.getenv("CSV_RUNTIME_LAYOUT"), "modular")
                    .trim()
                    .toLowerCase();
    private static final boolean MONOLITH_LAYOUT = "monolith".equals(RUNTIME_LAYOUT);
    private static final boolean PIPELINE_RUNTIME_LAYOUT = "pipeline-runtime".equals(RUNTIME_LAYOUT);
    private static final long ORCHESTRATOR_WAIT_TIMEOUT_SECONDS =
            Long.getLong("csv.e2e.orchestrator.wait.seconds", 300L);
    private static final long PIPELINE_WAIT_TIMEOUT_SECONDS =
            resolvePipelineWaitTimeoutSeconds();
    private static final long PIPELINE_WAIT_POLL_MILLIS = 1000L;
    private static final String PIPELINE_RUNTIME_IMAGE = resolvePipelineRuntimeImage();
    private static final boolean TELEMETRY_ENABLED =
            Boolean.getBoolean("csv.e2e.telemetry.enabled");
    private static final boolean TELEMETRY_CAPTURE_ACTIVE =
            TELEMETRY_ENABLED && !MONOLITH_LAYOUT && !PIPELINE_RUNTIME_LAYOUT;
    private static final boolean TEMPO_ENABLED =
            Boolean.getBoolean("csv.e2e.tempo.enabled");
    private static final boolean TEMPO_VERIFICATION_ACTIVE =
            TEMPO_ENABLED && !MONOLITH_LAYOUT && !PIPELINE_RUNTIME_LAYOUT;
    private static final boolean TEMPO_PAUSE_BEFORE_TEARDOWN =
            Boolean.getBoolean("csv.e2e.tempo.pause.before.teardown");
    private static final boolean TELEMETRY_HAPPY_PATH_ONLY =
            Boolean.parseBoolean(System.getProperty("csv.e2e.telemetry.happy-path-only", "true"));
    private static final String MODULAR_IMAGE_TAG = resolveModularImageTag();
    private static final String CSV_E2E_INPUT_FILE = System.getProperty("csv.e2e.input.file", "").trim();
    private static final boolean CUSTOM_INPUT_FILE = !CSV_E2E_INPUT_FILE.isBlank();
    private static volatile boolean orchestratorPackagingVerified;

    // Containers are lazily created so monolith mode does not require service cert binds.
    static PostgreSQLContainer<?> postgresContainer;
    static GenericContainer<?> persistenceService;
    static GenericContainer<?> inputCsvService;
    static GenericContainer<?> paymentsProcessingService;
    static GenericContainer<?> paymentStatusService;
    static GenericContainer<?> outputCsvService;
    static GenericContainer<?> pipelineRuntimeService;
    static GenericContainer<?> lgtmStack;
    static KafkaContainer kafkaContainer;

    private static long resolvePipelineWaitTimeoutSeconds() {
        long configured = Long.getLong("csv.e2e.pipeline.wait.seconds", 120L);
        if (PIPELINE_RUNTIME_LAYOUT) {
            return Math.max(configured, 240L);
        }
        return configured;
    }

    private static String resolvePipelineRuntimeImage() {
        String propertyImage = System.getProperty("csv.e2e.pipeline.runtime.image");
        if (propertyImage != null && !propertyImage.isBlank()) {
            return propertyImage;
        }

        String explicitImage = System.getenv("PIPELINE_RUNTIME_IMAGE");
        if (explicitImage != null && !explicitImage.isBlank()) {
            return explicitImage;
        }

        String registry = System.getenv().getOrDefault("IMAGE_REGISTRY", "registry.example.com");
        String group = System.getenv().getOrDefault("IMAGE_GROUP", "csv-payments");
        String name = System.getenv().getOrDefault("IMAGE_NAME", "pipeline-runtime-svc");
        String tag = System.getenv().getOrDefault("IMAGE_TAG", "1.0.0");
        return registry + "/" + group + "/" + name + ":" + tag;
    }

    private static String resolveModularImageTag() {
        if (TEMPO_VERIFICATION_ACTIVE) {
            return System.getProperty("csv.e2e.tempo.image.tag", "observability");
        }
        if (TELEMETRY_CAPTURE_ACTIVE) {
            return System.getProperty("csv.e2e.telemetry.image.tag", "otel");
        }
        return System.getProperty("csv.e2e.image.tag", "latest");
    }

    private static String modularImage(String serviceName) {
        return "localhost/csv-payments/" + serviceName + ":" + MODULAR_IMAGE_TAG;
    }

    private static void verifyModularServiceImagesMatchDockerArchitecture() {
        if (!Boolean.parseBoolean(System.getProperty("csv.e2e.verify.image.architecture", "true"))) {
            return;
        }
        String dockerArchitecture = normalizeDockerArchitecture(
                DockerClientFactory.instance().getInfo().getArchitecture());
        for (String image : List.of(
                modularImage("persistence-svc"),
                modularImage("input-csv-file-processing-svc"),
                modularImage("payments-processing-svc"),
                modularImage("payment-status-svc"))) {
            String imageArchitecture = normalizeDockerArchitecture(
                    DockerClientFactory.instance().client().inspectImageCmd(image).exec().getArch());
            assertEquals(
                    dockerArchitecture,
                    imageArchitecture,
                    "Image architecture for " + image + " must match Docker server architecture. "
                            + "Rebuild the CSV Payments modular images with "
                            + "./examples/csv-payments/build-modular-telemetry-images.sh "
                            + "-Dmaven.repo.local=\"$PWD/.m2/repository\" or set csv.e2e.image.tag to a matching image tag.");
        }
    }

    private static String normalizeDockerArchitecture(String architecture) {
        if (architecture == null || architecture.isBlank()) {
            return "";
        }
        return switch (architecture.trim().toLowerCase()) {
            case "x86_64" -> "amd64";
            case "aarch64" -> "arm64";
            default -> architecture.trim().toLowerCase();
        };
    }

    /**
         * Prepare test artifacts and start the containers required for the end-to-end CSV payments tests.
         *
         * Creates the test output directory (if missing) and ensures it is writable, ensures development
         * certificates are available, then starts the PostgreSQL container and the service containers.
         * In monolith layout only the PostgreSQL container is started.
         *
         * @throws IOException if creating directories, ensuring writability, or preparing development certificates fails
         */
    @BeforeAll
    static void startServices() throws IOException {
        assertFalse(
                TELEMETRY_CAPTURE_ACTIVE && TEMPO_VERIFICATION_ACTIVE,
                "Replay capture and Tempo verification modes are mutually exclusive.");
        // Create the test directory if it doesn't exist
        Path dir = Paths.get(TEST_E2E_DIR);
        Files.createDirectories(dir);
        ensureWritable(dir);
        ensurePackagedOrchestratorFresh();
        prepareObservabilityHarness();
        ensureDevCerts();

        if (MONOLITH_LAYOUT) {
            Startables.deepStart(Stream.of(getPostgresContainer(), getKafkaContainer())).join();
            ensureKafkaTopics();
            return;
        }

        if (PIPELINE_RUNTIME_LAYOUT) {
            Startables.deepStart(Stream.of(getPostgresContainer(), getKafkaContainer())).join();
            ensureKafkaTopics();
            Startables.deepStart(Stream.of(
                    getPersistenceService(),
                    getPipelineRuntimeService())).join();
            return;
        }

        if (TEMPO_VERIFICATION_ACTIVE) {
            Startables.deepStart(Stream.of(getPostgresContainer(), getKafkaContainer(), getLgtmStackContainer())).join();
        } else {
            Startables.deepStart(Stream.of(getPostgresContainer(), getKafkaContainer())).join();
        }
        ensureKafkaTopics();
        verifyModularServiceImagesMatchDockerArchitecture();

        Stream.Builder<GenericContainer<?>> services = Stream.builder();
        services.add(getPersistenceService());
        services.add(getInputCsvService());
        services.add(getPaymentsProcessingService());
        services.add(getPaymentStatusService());

        Startables.deepStart(services.build()).join();
        logObservabilityEndpoints();
    }

    /**
     * Obtain the lazily initialized PostgreSQL Testcontainers instance used by the tests.
     *
     * @return the configured {@code PostgreSQLContainer} (database name "quarkus", username/password "quarkus",
     *         attached to the test network, alias "postgres", with a 60-second startup wait)
     */
    private static PostgreSQLContainer<?> getPostgresContainer() {
        if (postgresContainer == null) {
            postgresContainer =
                    new PostgreSQLContainer<>("postgres:17")
                            .withDatabaseName("quarkus")
                            .withUsername("quarkus")
                            .withPassword("quarkus")
                            .withNetwork(network)
                            .withNetworkAliases("postgres")
                            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60)));
        }
        return postgresContainer;
    }

    private static KafkaContainer getKafkaContainer() {
        if (kafkaContainer == null) {
            kafkaContainer =
                    new KafkaContainer(KAFKA_IMAGE)
                            .withNetwork(network)
                            .withNetworkAliases(KAFKA_NETWORK_ALIAS)
                            .withListener(KAFKA_NETWORK_BOOTSTRAP)
                            .withLogConsumer(containerLog("kafka"))
                            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(90)));
        }
        return kafkaContainer;
    }

    /**
     * Creates and returns the test container configured for the persistence service used in end-to-end tests.
     *
     * The container is lazily initialized on first invocation and is configured with network settings,
     * SSL keystore binding, exposed port, datasource credentials, and an HTTPS health check.
     *
     * @return the initialized GenericContainer instance for the persistence service (lazily created)
     */
    private static GenericContainer<?> getPersistenceService() {
        if (persistenceService == null) {
            persistenceService =
                    new GenericContainer<>(modularImage("persistence-svc"))
                            .withNetwork(network)
                            .withNetworkAliases("persistence-svc")
                            .withFileSystemBind(
                                    DEV_CERTS_DIR.resolve("persistence-svc/server-keystore.jks").toString(),
                                    CONTAINER_KEYSTORE_PATH,
                                    BindMode.READ_ONLY)
                            .withExposedPorts(8448)
                            .withEnv("QUARKUS_PROFILE", "test")
                            .withEnv("SERVER_KEYSTORE_PATH", CONTAINER_KEYSTORE_PATH)
                            .withEnv(
                                    "QUARKUS_HTTP_SSL_CERTIFICATE_KEY_STORE_FILE",
                                    CONTAINER_KEYSTORE_PATH)
                            .withEnv(
                                    "QUARKUS_DATASOURCE_REACTIVE_URL", "postgresql://postgres:5432/quarkus")
                            .withEnv("QUARKUS_DATASOURCE_USERNAME", "quarkus")
                            .withEnv("QUARKUS_DATASOURCE_PASSWORD", "quarkus")
                            .withLogConsumer(containerLog("persistence-svc"))
                            .waitingFor(
                                    Wait.forHttps("/q/health")
                                            .forPort(8448)
                                            .allowInsecure()
                                            .withStartupTimeout(Duration.ofSeconds(60)));
            if (TELEMETRY_CAPTURE_ACTIVE) {
                persistenceService.withFileSystemBind(
                        REPLAY_CAPTURE_DIR.toAbsolutePath().toString(),
                        REPLAY_CAPTURE_CONTAINER_DIR,
                        BindMode.READ_WRITE);
            }
            configureObservabilityContainerEnv(persistenceService);
        }
        return persistenceService;
    }

    /**
     * Lazily initialize the Testcontainers container for the input CSV file processing service used by end-to-end tests.
     *
     * @return the configured GenericContainer instance for the input CSV file processing service
     */
    private static GenericContainer<?> getInputCsvService() {
        if (inputCsvService == null) {
            inputCsvService =
                    new GenericContainer<>(modularImage("input-csv-file-processing-svc"))
                            .withNetwork(network)
                            .withNetworkAliases("input-csv-file-processing-svc")
                            .withFileSystemBind(
                                    Paths.get(TEST_E2E_DIR).toAbsolutePath().toString(),
                                    TEST_E2E_TARGET_DIR,
                                    BindMode.READ_ONLY)
                            .withFileSystemBind(
                                    DEV_CERTS_DIR.resolve("input-csv-file-processing-svc/server-keystore.jks")
                                            .toString(),
                                    CONTAINER_KEYSTORE_PATH,
                                    BindMode.READ_ONLY)
                            .withExposedPorts(8444)
                            .withEnv("QUARKUS_PROFILE", "test")
                            .withEnv("SERVER_KEYSTORE_PATH", CONTAINER_KEYSTORE_PATH)
                            .withLogConsumer(containerLog("input-csv-file-processing-svc"))
                            .waitingFor(
                                    Wait.forHttps("/q/health/live")
                                            .forPort(8444)
                                            .allowInsecure()
                                            .withStartupTimeout(Duration.ofSeconds(60)));
            configureObservabilityContainerEnv(inputCsvService);
        }
        return inputCsvService;
    }

    /**
     * Lazily creates and returns a Testcontainers GenericContainer configured for the payments-processing service.
     *
     * The container is attached to the shared test network, exposes port 8445, mounts the service keystore
     * into the container, sets the Quarkus profile to "test", and waits for an HTTPS liveness endpoint at
     * /q/health/live.
     *
     * @return the initialized or previously created GenericContainer for the payments-processing service
     */
    private static GenericContainer<?> getPaymentsProcessingService() {
        if (paymentsProcessingService == null) {
            paymentsProcessingService =
                    new GenericContainer<>(modularImage("payments-processing-svc"))
                            .withNetwork(network)
                            .withNetworkAliases("payments-processing-svc")
                            .withFileSystemBind(
                                    DEV_CERTS_DIR.resolve("payments-processing-svc/server-keystore.jks")
                                            .toString(),
                                    CONTAINER_KEYSTORE_PATH,
                                    BindMode.READ_ONLY)
                            .withExposedPorts(8445)
                            .withEnv("QUARKUS_PROFILE", "test")
                            .withEnv("SERVER_KEYSTORE_PATH", CONTAINER_KEYSTORE_PATH)
                            .withEnv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_NETWORK_BOOTSTRAP)
                            .withLogConsumer(containerLog("payments-processing-svc"))
                            .waitingFor(
                                    Wait.forHttps("/q/health/live")
                                            .forPort(8445)
                                            .allowInsecure()
                                            .withStartupTimeout(Duration.ofSeconds(60)));
            if (TELEMETRY_CAPTURE_ACTIVE) {
                paymentsProcessingService.withFileSystemBind(
                        REPLAY_CAPTURE_DIR.toAbsolutePath().toString(),
                        REPLAY_CAPTURE_CONTAINER_DIR,
                        BindMode.READ_WRITE);
            }
            configureObservabilityContainerEnv(paymentsProcessingService);
        }
        return paymentsProcessingService;
    }

    /**
     * Create and configure the payment status service Testcontainers GenericContainer for the test network.
     *
     * The container is lazily initialized and configured with network settings, a mounted server keystore,
     * exposed HTTPS port 8446, the `test` Quarkus profile, and a liveness check against
     * `/q/health/live`.
     *
     * @return the configured GenericContainer instance for the payment status service
     */
    private static GenericContainer<?> getPaymentStatusService() {
        if (paymentStatusService == null) {
            paymentStatusService =
                    new GenericContainer<>(modularImage("payment-status-svc"))
                            .withNetwork(network)
                            .withNetworkAliases("payment-status-svc")
                            .withFileSystemBind(
                                    DEV_CERTS_DIR.resolve("payment-status-svc/server-keystore.jks").toString(),
                                    CONTAINER_KEYSTORE_PATH,
                                    BindMode.READ_ONLY)
                            .withExposedPorts(8446)
                            .withEnv("QUARKUS_PROFILE", "test")
                            .withEnv("SERVER_KEYSTORE_PATH", CONTAINER_KEYSTORE_PATH)
                            .withLogConsumer(containerLog("payment-status-svc"))
                            .waitingFor(
                                    Wait.forHttps("/q/health/live")
                                            .forPort(8446)
                                            .allowInsecure()
                                            .withStartupTimeout(Duration.ofSeconds(60)));
            if (TELEMETRY_CAPTURE_ACTIVE) {
                paymentStatusService.withFileSystemBind(
                        REPLAY_CAPTURE_DIR.toAbsolutePath().toString(),
                        REPLAY_CAPTURE_CONTAINER_DIR,
                        BindMode.READ_WRITE);
            }
            configureObservabilityContainerEnv(paymentStatusService);
        }
        return paymentStatusService;
    }

    /**
     * Lazily creates and returns a Testcontainers GenericContainer configured for the output CSV file
     * processing service.
     *
     * <p>The returned container mounts the test output directory into the container, binds the
     * service keystore for TLS, exposes the service port (8447), sets the Quarkus test profile and
     * server keystore environment variables, attaches a log consumer, and waits for the HTTPS
     * health endpoint to become available.
     *
     * @return the initialized GenericContainer configured for the output CSV file processing service
     */
    private static GenericContainer<?> getOutputCsvService() {
        if (outputCsvService == null) {
            outputCsvService =
                    new GenericContainer<>(modularImage("output-csv-file-processing-svc"))
                            .withNetwork(network)
                            .withNetworkAliases("output-csv-file-processing-svc")
                            .withFileSystemBind(
                                    Paths.get(TEST_E2E_DIR).toAbsolutePath().toString(),
                                    TEST_E2E_TARGET_DIR,
                                    BindMode.READ_WRITE)
                            .withFileSystemBind(
                                    DEV_CERTS_DIR.resolve("output-csv-file-processing-svc/server-keystore.jks")
                                            .toString(),
                                    CONTAINER_KEYSTORE_PATH,
                                    BindMode.READ_ONLY)
                            .withExposedPorts(8447)
                            .withEnv("QUARKUS_PROFILE", "test")
                            .withEnv("SERVER_KEYSTORE_PATH", CONTAINER_KEYSTORE_PATH)
                            .withLogConsumer(containerLog("output-csv-file-processing-svc"))
                            .waitingFor(
                                    Wait.forHttps("/q/health/live")
                                            .forPort(8447)
                                            .allowInsecure()
                                            .withStartupTimeout(Duration.ofSeconds(60)));
            configureObservabilityContainerEnv(outputCsvService);
        }
        return outputCsvService;
    }

    /**
     * Lazily creates and returns the grouped pipeline runtime service container used by pipeline-runtime layout tests.
     *
     * <p>The container mounts the test directory read-write (the grouped runtime both reads CSV input and writes
     * output files), binds a server keystore for TLS, and waits for HTTPS health on port 8445.
     *
     * @return the configured grouped pipeline runtime container
     */
    private static GenericContainer<?> getPipelineRuntimeService() {
        if (pipelineRuntimeService == null) {
            pipelineRuntimeService =
                    new GenericContainer<>(PIPELINE_RUNTIME_IMAGE)
                            .withNetwork(network)
                            .withNetworkAliases("pipeline-runtime-svc")
                            .withFileSystemBind(
                                    Paths.get(TEST_E2E_DIR).toAbsolutePath().toString(),
                                    TEST_E2E_TARGET_DIR,
                                    BindMode.READ_WRITE)
                            .withFileSystemBind(
                                    DEV_CERTS_DIR.resolve("pipeline-runtime-svc/server-keystore.jks")
                                            .toString(),
                                    CONTAINER_KEYSTORE_PATH,
                                    BindMode.READ_ONLY)
                            .withFileSystemBind(
                                    DEV_CERTS_DIR.resolve("pipeline-runtime-svc/client-truststore.jks")
                                            .toString(),
                                    CONTAINER_TRUSTSTORE_PATH,
                                    BindMode.READ_ONLY)
                            .withExposedPorts(8445)
                            .withEnv("QUARKUS_PROFILE", "test")
                            .withEnv("SERVER_KEYSTORE_PATH", CONTAINER_KEYSTORE_PATH)
                            .withEnv("SERVER_KEYSTORE_PASSWORD", "secret")
                            .withEnv("CLIENT_TRUSTSTORE_PATH", CONTAINER_TRUSTSTORE_PATH)
                            .withEnv("CLIENT_TRUSTSTORE_PASSWORD", "secret")
                            // Keep the pipeline-runtime lane on the same low-retry, bounded-concurrency
                            // settings used by the other E2E topologies regardless of image defaults.
                            .withEnv("PIPELINE_MAX_CONCURRENCY", "128")
                            .withEnv("PIPELINE_DEFAULTS_RETRY_LIMIT", "3")
                            .withEnv("PIPELINE_DEFAULTS_RETRY_WAIT_MS", "100")
                            .withEnv("PIPELINE_ORCHESTRATOR_MODE", "QUEUE_ASYNC")
                            .withEnv("PIPELINE_ORCHESTRATOR_RESUME_TOKEN_SECRET", E2E_RESUME_TOKEN_SECRET)
                            .withEnv("PIPELINE_ORCHESTRATOR_IDEMPOTENCY_POLICY", "CLIENT_KEY_REQUIRED")
                            .withEnv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_NETWORK_BOOTSTRAP)
                            .withLogConsumer(containerLog("pipeline-runtime-svc"))
                            .waitingFor(
                                    Wait.forHttps("/q/health")
                                            .forPort(8445)
                                            .allowInsecure()
                                            .withStartupTimeout(Duration.ofSeconds(60)));
            configureObservabilityContainerEnv(pipelineRuntimeService);
        }
        return pipelineRuntimeService;
    }

    private static GenericContainer<?> getLgtmStackContainer() {
        if (lgtmStack == null) {
            lgtmStack =
                    new GenericContainer<>(LGTM_IMAGE)
                            .withNetwork(network)
                            .withNetworkAliases("lgtm", "otel-collector")
                            .withExposedPorts(3000, 3200, 4318)
                            .withLogConsumer(containerLog("lgtm"))
                            .waitingFor(Wait.forListeningPort().withStartupTimeout(LGTM_STARTUP_TIMEOUT));
        }
        return lgtmStack;
    }

    /**
     * Creates a Consumer that logs non-empty container output frames prefixed with the given container name.
     *
     * @param containerName label used as a tag at the start of each log message
     * @return a Consumer that logs each non-null, non-blank OutputFrame's UTF-8 text prefixed by the container name
     */
    private static Consumer<OutputFrame> containerLog(String containerName) {
        return outputFrame -> {
            if (outputFrame == null) {
                return;
            }
            String message = outputFrame.getUtf8String();
            if (message == null || message.isBlank()) {
                return;
            }
            LOG.infof("[%s] %s", containerName, message.trim());
        };
    }

    private static void prepareObservabilityHarness() throws IOException {
        if (TEMPO_VERIFICATION_ACTIVE) {
            LOG.info("CSV payments Tempo verification enabled; OTLP export will target the dedicated LGTM stack.");
            return;
        }
        if (TEMPO_ENABLED) {
            LOG.warnf(
                    "CSV payments Tempo verification currently supports only the modular layout; active layout is %s.",
                    RUNTIME_LAYOUT);
        }
        prepareTelemetryCapture();
    }

    private static void prepareTelemetryCapture() throws IOException {
        if (!TELEMETRY_CAPTURE_ACTIVE) {
            if (TELEMETRY_ENABLED) {
                LOG.warnf(
                        "CSV payments OTel telemetry capture currently supports only the modular layout; active layout is %s.",
                        RUNTIME_LAYOUT);
            }
            return;
        }
        Files.createDirectories(REPLAY_ROOT_DIR);
        clearReplayCaptureDirectory();
        Files.deleteIfExists(REPLAY_FILE);
        LOG.infof(
                "CSV payments replay export enabled; capturing replay fragments under %s and merged replay at %s.",
                REPLAY_CAPTURE_DIR,
                REPLAY_FILE);
    }

    private static void configureObservabilityContainerEnv(GenericContainer<?> container) {
        Map<String, String> env = new LinkedHashMap<>();
        configureObservabilityContainerEnv(env);
        env.forEach(container::withEnv);
    }

    private static void configureObservabilityContainerEnv(Map<String, String> env) {
        configureProviderOverrideEnv(env);
        if (TEMPO_VERIFICATION_ACTIVE) {
            configureTempoEnv(env, lgtmCollectorContainerEndpoint());
            return;
        }
        configureReplayCaptureEnv(env, REPLAY_CAPTURE_CONTAINER_DIR);
    }

    private static void configureObservabilityProcessEnv(Map<String, String> env) {
        configureProviderOverrideEnv(env);
        if (TEMPO_VERIFICATION_ACTIVE) {
            configureTempoEnv(env, lgtmCollectorHostEndpoint());
            return;
        }
        configureReplayCaptureEnv(env, REPLAY_CAPTURE_DIR.toAbsolutePath().toString());
    }

    private static void configureProviderOverrideEnv(Map<String, String> env) {
        putOptionalEnvOverride(
                env,
                "csv-payments.payment-provider.permits-per-second",
                "CSV_PAYMENTS_PAYMENT_PROVIDER_PERMITS_PER_SECOND");
        putOptionalEnvOverride(
                env,
                "csv-payments.payment-provider.timeout-millis",
                "CSV_PAYMENTS_PAYMENT_PROVIDER_TIMEOUT_MILLIS");
        putOptionalEnvOverride(
                env,
                "csv-payments.payment-provider.provider-timeout-probability",
                "CSV_PAYMENTS_PAYMENT_PROVIDER_PROVIDER_TIMEOUT_PROBABILITY");
        putOptionalEnvOverride(
                env,
                "csv-payments.payment-provider.provider-reject-probability",
                "CSV_PAYMENTS_PAYMENT_PROVIDER_PROVIDER_REJECT_PROBABILITY");
    }

    private static void configureReplayCaptureEnv(Map<String, String> env, String replayCapturePath) {
        if (!TELEMETRY_CAPTURE_ACTIVE) {
            return;
        }
        env.put("QUARKUS_OTEL_ENABLED", "true");
        env.put("QUARKUS_OTEL_SDK_DISABLED", "false");
        env.put("QUARKUS_OTEL_METRICS_ENABLED", "false");
        env.put("QUARKUS_OTEL_TRACES_ENABLED", "true");
        env.put("QUARKUS_OTEL_LOGS_ENABLED", "false");
        env.put("QUARKUS_OTEL_EXPORTER_OTLP_ENABLED", "false");
        env.put("QUARKUS_OTEL_TRACES_SAMPLER", "parentbased_always_on");
        env.put("QUARKUS_OTEL_TRACES_SAMPLER_ARG", "1.0");
        env.put("QUARKUS_OBSERVABILITY_LGTM_ENABLED", "false");
        env.put("PIPELINE_TELEMETRY_ENABLED", "true");
        env.put("PIPELINE_TELEMETRY_TRACING_ENABLED", "true");
        env.put("PIPELINE_TELEMETRY_TRACING_PER_ITEM", "true");
        env.put("PIPELINE_TELEMETRY_METRICS_ENABLED", "false");
        env.put("PIPELINE_TELEMETRY_REPLAY_ENABLED", "true");
        env.put("PIPELINE_TELEMETRY_REPLAY_EXPORTER", "file");
        env.put("PIPELINE_TELEMETRY_REPLAY_FILE_PATH", replayCapturePath);
    }

    private static void configureTempoEnv(Map<String, String> env, String otlpEndpoint) {
        if (!TEMPO_VERIFICATION_ACTIVE) {
            return;
        }
        env.put("QUARKUS_OTEL_ENABLED", "true");
        env.put("QUARKUS_OTEL_SDK_DISABLED", "false");
        env.put("QUARKUS_OTEL_METRICS_ENABLED", "false");
        env.put("QUARKUS_OTEL_TRACES_ENABLED", "true");
        env.put("QUARKUS_OTEL_LOGS_ENABLED", "false");
        env.put("QUARKUS_OTEL_EXPORTER_OTLP_ENABLED", "true");
        env.put("QUARKUS_OTEL_EXPORTER_OTLP_ENDPOINT", otlpEndpoint);
        env.put("QUARKUS_OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf");
        env.put("QUARKUS_OTEL_TRACES_SAMPLER", "parentbased_always_on");
        env.put("QUARKUS_OTEL_TRACES_SAMPLER_ARG", "1.0");
        env.put("QUARKUS_OBSERVABILITY_LGTM_ENABLED", "false");
        env.put("PIPELINE_TELEMETRY_ENABLED", "true");
        env.put("PIPELINE_TELEMETRY_TRACING_ENABLED", "true");
        env.put("PIPELINE_TELEMETRY_TRACING_PER_ITEM", "true");
        env.put("PIPELINE_TELEMETRY_METRICS_ENABLED", "false");
    }

    private static String lgtmCollectorContainerEndpoint() {
        return "http://otel-collector:4318";
    }

    private static String lgtmCollectorHostEndpoint() {
        return "http://localhost:" + getLgtmStackContainer().getMappedPort(4318);
    }

    private static String tempoApiBaseUrl() {
        return "http://localhost:" + getLgtmStackContainer().getMappedPort(3200);
    }

    private static String grafanaBaseUrl() {
        return "http://localhost:" + getLgtmStackContainer().getMappedPort(3000);
    }

    private static void logObservabilityEndpoints() {
        if (!TEMPO_VERIFICATION_ACTIVE) {
            return;
        }
        LOG.infof("Grafana UI available at %s.", grafanaBaseUrl());
        LOG.infof("Tempo API available at %s.", tempoApiBaseUrl());
        LOG.infof("OTLP collector endpoint available at %s.", lgtmCollectorHostEndpoint());
    }

    private static void putOptionalEnvOverride(Map<String, String> env, String propertyName, String envName) {
        String value = System.getProperty(propertyName, "").trim();
        if (!value.isBlank()) {
            env.put(envName, value);
        }
    }

    /**
     * Attempts to set POSIX file permissions to make the given path writable.
     *
     * If the path is a directory, sets permissions to `rwxr-xr-x`; otherwise sets `rw-r--r--`.
     * A warning is logged if the permissions cannot be changed or the platform does not support POSIX permissions.
     *
     * @param path the file or directory to make writable
     */
    private static void ensureWritable(Path path) {
        try {
            // Containers may run as non-root/non-host UID, so mounted test paths must be writable by all.
            String permission = Files.isDirectory(path) ? "rwxrwxrwx" : "rw-rw-rw-";
            Set<PosixFilePermission> perms = PosixFilePermissions.fromString(permission);
            Files.setPosixFilePermissions(path, perms);
        } catch (IOException | UnsupportedOperationException e) {
            LOG.warnf("Unable to set permissions on %s: %s", path, e.getMessage());
        }
    }

    private static void ensurePackagedOrchestratorFresh() throws IOException {
        if (PIPELINE_RUNTIME_LAYOUT || orchestratorPackagingVerified) {
            return;
        }
        synchronized (AbstractCsvPaymentsEndToEnd.class) {
            if (PIPELINE_RUNTIME_LAYOUT || orchestratorPackagingVerified) {
                return;
            }
            String jarPath =
                    MONOLITH_LAYOUT
                            ? "../monolith-svc/target/quarkus-app/quarkus-run.jar"
                            : "target/quarkus-app/quarkus-run.jar";
            Path jar = resolveJarPath(jarPath, MONOLITH_LAYOUT);
            if (needsPackagedRefresh(jar)) {
                LOG.infof("Packaged orchestrator at %s is stale or missing; rebuilding executable jar.", jar);
                rebuildPackagedOrchestrator();
            }
            orchestratorPackagingVerified = true;
        }
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "";
    }

    private static boolean needsPackagedRefresh(Path jar) throws IOException {
        if (!Files.isRegularFile(jar)) {
            return true;
        }
        long packagedAt = Files.getLastModifiedTime(jar).toMillis();
        long classesAt = latestModifiedUnder(Paths.get(System.getProperty("user.dir")).resolve("target/classes"));
        long mainSourcesAt = latestModifiedUnder(Paths.get(System.getProperty("user.dir")).resolve("src/main"));
        long pomAt = Files.getLastModifiedTime(Paths.get(System.getProperty("user.dir")).resolve("pom.xml")).toMillis();
        long runtimeAt = codeSourceModified(PipelineTelemetry.class);
        long latestDependencyAt = Math.max(runtimeAt, pomAt);
        long latestLocalAt = Math.max(classesAt, mainSourcesAt);
        return packagedAt < latestDependencyAt || packagedAt < latestLocalAt;
    }

    private static long latestModifiedUnder(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return 0L;
        }
        if (Files.isRegularFile(root)) {
            return Files.getLastModifiedTime(root).toMillis();
        }
        try (Stream<Path> files = Files.walk(root)) {
            return files.filter(Files::isRegularFile)
                    .mapToLong(
                            path -> {
                                try {
                                    return Files.getLastModifiedTime(path).toMillis();
                                } catch (IOException e) {
                                    return 0L;
                                }
                            })
                    .max()
                    .orElse(0L);
        }
    }

    private static long codeSourceModified(Class<?> type) {
        try {
            if (type == null
                    || type.getProtectionDomain() == null
                    || type.getProtectionDomain().getCodeSource() == null
                    || type.getProtectionDomain().getCodeSource().getLocation() == null) {
                return 0L;
            }
            Path location = Paths.get(type.getProtectionDomain().getCodeSource().getLocation().toURI());
            if (!Files.exists(location)) {
                return 0L;
            }
            return Files.getLastModifiedTime(location).toMillis();
        } catch (Exception e) {
            LOG.debugf(e, "Unable to determine code-source timestamp for %s.", type);
            return 0L;
        }
    }

    private static void rebuildPackagedOrchestrator() throws IOException {
        Path moduleDir = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
        Path processDir = moduleDir;
        List<String> command = new ArrayList<>();
        String mavenRepoLocal = System.getProperty("maven.repo.local", "").trim();
        if (MONOLITH_LAYOUT) {
            processDir = moduleDir.getParent();
            command.add("bash");
            command.add("build-monolith.sh");
            command.add("-DskipTests");
            command.add("-Dquarkus.container-image.build=false");
            command.add("-Dquarkus.container-image.push=false");
            if (!mavenRepoLocal.isBlank()) {
                command.add("-Dmaven.repo.local=" + mavenRepoLocal);
            }
        } else {
            command.add("../../../mvnw");
            command.add("-f");
            command.add("../pom.xml");
            command.add("-pl");
            command.add("orchestrator-svc");
            command.add("-am");
            command.add("-DskipTests");
            command.add("-Dquarkus.container-image.build=false");
            command.add("-Dquarkus.container-image.push=false");
            if (!mavenRepoLocal.isBlank()) {
                command.add("-Dmaven.repo.local=" + mavenRepoLocal);
            }
            command.add("package");
        }
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(processDir.toFile());
        pb.environment().put("CSV_RUNTIME_LAYOUT", RUNTIME_LAYOUT);
        if (!mavenRepoLocal.isBlank()) {
            String existingMavenArgs = pb.environment().getOrDefault("MAVEN_ARGS", "").trim();
            String repoArg = "-Dmaven.repo.local=" + mavenRepoLocal;
            pb.environment().put(
                    "MAVEN_ARGS",
                    existingMavenArgs.isBlank() ? repoArg : existingMavenArgs + " " + repoArg);
        }
        pb.redirectErrorStream(true);
        Process process = pb.start();
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append(System.lineSeparator());
            }
        }
        try {
            int exitCode = process.waitFor();
            assertEquals(
                    0,
                    exitCode,
                    "Failed to rebuild packaged orchestrator. Output tail:\n" + tailLines(output.toString(), 120));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while rebuilding packaged orchestrator.", e);
        }
    }

    /**
     * Ensures development TLS certificates required by the active test topology exist.
     *
     * <p>If required cert files are missing under the configured dev certificates directory, this
     * method invokes the repository script "../generate-dev-certs.sh" to create them.
     *
     * @throws IOException if certificate files are missing and cannot be generated, including when
     *                     running on Windows (generation is supported only on Unix-like systems),
     *                     if the generation script exits with a non-zero status, or if the
     *                     generation process is interrupted.
     */
    private static void ensureDevCerts() throws IOException {
        Path orchestratorKeystore = DEV_CERTS_DIR.resolve("orchestrator-svc/server-keystore.jks");
        Path orchestratorTruststore = DEV_CERTS_DIR.resolve("orchestrator-svc/client-truststore.jks");
        Path pipelineRuntimeKeystore = DEV_CERTS_DIR.resolve("pipeline-runtime-svc/server-keystore.jks");
        Path pipelineRuntimeTruststore = DEV_CERTS_DIR.resolve("pipeline-runtime-svc/client-truststore.jks");

        boolean orchestratorCertsReady =
                Files.exists(orchestratorKeystore) && Files.exists(orchestratorTruststore);
        boolean pipelineRuntimeCertsReady =
                Files.exists(pipelineRuntimeKeystore) && Files.exists(pipelineRuntimeTruststore);

        if (orchestratorCertsReady && (!PIPELINE_RUNTIME_LAYOUT || pipelineRuntimeCertsReady)) {
            return;
        }

        String osName = System.getProperty("os.name", "");
        if (osName.toLowerCase().contains("win")) {
            throw new IOException(
                    "Dev cert generation is Unix-only. Run ../generate-dev-certs.sh from a Unix-like shell "
                            + "(e.g. Linux/macOS/WSL) before running this test.");
        }
        ProcessBuilder pb = new ProcessBuilder("bash", "../generate-dev-certs.sh");
        pb.directory(Paths.get(System.getProperty("user.dir")).toFile());
        pb.inheritIO();
        try {
            Process process = pb.start();
            boolean completed = process.waitFor(60, TimeUnit.SECONDS);
            assertTrue(completed, "Dev cert generation timed out");
            assertEquals(0, process.exitValue(), "Dev cert generation failed");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while generating dev certs", e);
        }
    }

    /**
     * Runs a full end-to-end integration test of the CSV payments processing pipeline.
     *
     * <p>Sets up the test input directory and CSV files, invokes the orchestrator to process them,
     * waits for pipeline completion, and verifies both generated output files and database
     * persistence.
     */
    @Test
    void fullPipelineWorks() throws Exception {
        assumeTrue(runHappyPathScenario(), "Happy-path scenario disabled for this E2E class.");
        LOG.info("Running full end-to-end pipeline test");

        executeHappyPathPipelineAndVerify();

        LOG.info("End-to-end processing test completed successfully!");
    }

    @Test
    void tempoSpansReachLgtm() throws Exception {
        assumeTrue(runTempoVerificationScenario(), "Tempo verification scenario disabled for this E2E class.");
        assumeTrue(
                TEMPO_VERIFICATION_ACTIVE,
                "Tempo verification scenario requires csv.e2e.tempo.enabled in modular layout.");
        LOG.info("Running Tempo/LGTM verification end-to-end test");

        executeHappyPathPipelineAndVerify();
        assertTempoTracesAvailable();

        LOG.info("Tempo/LGTM verification test completed successfully!");
    }

    private void executeHappyPathPipelineAndVerify() throws Exception {
        LOG.info("Executing happy-path pipeline run and verifying outputs.");

        // Create test input directory - already created in @BeforeAll
        Path dir = Paths.get(TEST_E2E_DIR);
        // Make sure the directory exists at the time of test execution too
        Files.createDirectories(dir);

        // Clean up any existing test files
        cleanTestOutputDirectory(dir);
        resetReplayCaptureArtifacts();
        resetDatabasePersistence();

        // Create test CSV files as the shell script does
        createTestCsvFiles();

        orchestratorTriggerRun();

        // Wait for the pipeline to complete
        waitForPipelineComplete();

        // Verify the output files are generated with expected content
        verifyOutputFiles(TEST_E2E_DIR);

        // Verify database persistence
        verifyDatabasePersistence();

        if (TELEMETRY_CAPTURE_ACTIVE) {
            PipelineReplayDocument replayDocument = mergeReplayDocuments(REPLAY_CAPTURE_DIR, REPLAY_FILE);
            assertReplayCoverage(replayDocument);
        }
    }

    @Test
    void pipelineContinuesWhenMalformedCsvIsRejected() throws Exception {
        assumeTrue(runMalformedRejectScenario(), "Malformed reject scenario disabled for this E2E class.");
        assumeFalse(
                TELEMETRY_CAPTURE_ACTIVE && TELEMETRY_HAPPY_PATH_ONLY,
                "Telemetry capture mode runs only the happy-path E2E by default.");
        LOG.info("Running malformed-input reject-and-continue end-to-end test");

        Path dir = Paths.get(TEST_E2E_DIR);
        Files.createDirectories(dir);
        cleanTestOutputDirectory(dir);
        resetDatabasePersistence();

        createValidAndMalformedCsvFiles();

        ProcessRunResult runResult = orchestratorTriggerRun(
            Map.of(
                "PIPELINE_DEFAULTS_RECOVER_ON_FAILURE", "true",
                "PIPELINE_DEFAULTS_RETRY_LIMIT", "1",
                "PIPELINE_DEFAULTS_RETRY_WAIT_MS", "10",
                "PIPELINE_ITEM_REJECT_PROVIDER", "memory"));

        waitForPipelineComplete();

        Set<String> expectedRecipients = Set.of("Valid Recipient One", "Valid Recipient Two");
        verifyOutputFilesForRecipients(TEST_E2E_DIR, expectedRecipients, "Malformed Recipient", 2);
        verifyDatabasePersistenceForRecipients(expectedRecipients, "Malformed Recipient", 2);

        assertTrue(
            runResult.output().contains("Item reject stored in memory sink"),
            "Expected in-memory item reject sink evidence in orchestrator logs.");
        assertTrue(
            runResult.output().contains("ProcessCsvPaymentsInputGrpcClientStep")
                || runResult.output().contains("ProcessCsvPaymentsInputService"),
            "Expected reject logs to reference ProcessCsvPaymentsInput step metadata.");
        assertTrue(
            runResult.output().contains("scope=STREAM"),
            "Expected stream-scope reject log entry for malformed CSV input.");

        LOG.info("Malformed-input reject-and-continue end-to-end test completed successfully!");
    }

    @Test
    void providerRejectsStillProduceOutputRows() throws Exception {
        assumeTrue(runProviderRejectScenario(), "Provider-reject scenario disabled for this E2E class.");
        assumeTrue(
                TELEMETRY_CAPTURE_ACTIVE,
                "Provider-reject replay assertions require telemetry capture in modular layout.");
        assumeFalse(
                TELEMETRY_HAPPY_PATH_ONLY,
                "Telemetry capture mode runs only the happy-path E2E by default.");
        assumeTrue(
                configuredProviderRejectProbability() > 0.0d,
                "Provider-reject scenario requires csv-payments.payment-provider.provider-reject-probability > 0.");
        LOG.info("Running provider-reject end-to-end test");

        Path dir = Paths.get(TEST_E2E_DIR);
        Files.createDirectories(dir);
        cleanTestOutputDirectory(dir);
        resetReplayCaptureArtifacts();
        resetDatabasePersistence();
        createTestCsvFiles();

        orchestratorTriggerRun();

        waitForPipelineComplete();

        PipelineReplayDocument replayDocument = mergeReplayDocuments(REPLAY_CAPTURE_DIR, REPLAY_FILE);
        long outputRecords = outputRecordCount(Paths.get(TEST_E2E_DIR));
        assertEquals(expectedPaymentRecordCount(), outputRecords, "Expected one output row per valid input payment.");

        String combinedOutput = readCombinedOutput(TEST_E2E_DIR);
        long expectedRejects = expectedProviderRejectCount(configuredProviderRejectProbability());
        long actualRejectRows = combinedOutput.lines()
                .filter(line -> line.contains("Mock payment provider rejected the payment."))
                .count();
        long actualApprovedRows = combinedOutput.lines()
                .filter(line -> line.contains("Mock response"))
                .count();
        assertEquals(expectedRejects, actualRejectRows, "Expected one output row per deterministically rejected payment.");
        assertEquals(
                expectedPaymentRecordCount() - expectedRejects,
                actualApprovedRows,
                "Expected the remaining rows to stay on the approved output path.");
        assertTrue(actualRejectRows > 0, "Expected provider rejection text to be rendered into output rows.");
        assertTrue(actualApprovedRows > 0, "Expected at least one successful recipient to reach output.");
    }

    protected boolean runHappyPathScenario() {
        return true;
    }

    protected boolean runMalformedRejectScenario() {
        return true;
    }

    protected boolean runProviderRejectScenario() {
        return false;
    }

    protected boolean runTempoVerificationScenario() {
        return false;
    }

    /**
     * Start the orchestrator JAR in a separate JVM configured to run one object ingest poll.
     *
     * @throws Exception if the process cannot be started, times out, or exits with a non-zero exit code
     */
    private ProcessRunResult orchestratorTriggerRun() throws Exception {
        return orchestratorTriggerRun(Map.of());
    }

    private ProcessRunResult orchestratorTriggerRun(Map<String, String> envOverrides) throws Exception {
        LOG.info("Triggering Orchestrator object ingest once");

        String jarPath =
                MONOLITH_LAYOUT
                        ? "../monolith-svc/target/quarkus-app/quarkus-run.jar"
                        : "target/quarkus-app/quarkus-run.jar";
        // The test harness lives in orchestrator-svc, but monolith mode executes monolith-svc.
        Path jar = resolveJarPath(jarPath, MONOLITH_LAYOUT);
        String buildHint =
                MONOLITH_LAYOUT
                        ? "Run ./examples/csv-payments/build-monolith.sh -DskipTests first."
                        : "Run ./mvnw -f examples/csv-payments/pom.xml -pl orchestrator-svc -DskipTests package first.";
        if (!Files.isRegularFile(jar)) {
            LOG.infof(
                    "Resolved jar path: %s (exists=%s, regular=%s, size=%s)",
                    jar,
                    Files.exists(jar),
                    Files.isRegularFile(jar),
                    Files.exists(jar) ? Files.size(jar) : "n/a");
        }
        assertTrue(
                Files.isRegularFile(jar),
                "Expected executable jar at " + jarPath + ". " + buildHint);

        ProcessBuilder pb =
                new ProcessBuilder(
                        "java",
                        "--enable-preview",
                        "-jar",
                        jar.toString(),
                        "--ingest-once");

        pb.environment().put("QUARKUS_PROFILE", "test");
        pb.environment().put("PIPELINE_CONFIG", writeE2ePipelineConfig().toString());
        pb.environment().put("PIPELINE_OBJECT_INGEST_AUTOSTART", "false");
        pb.environment().put("QUARKUS_JIB_JVM_ADDITIONAL_ARGUMENTS", "--enable-preview");
        pb.environment().put("PIPELINE_ORCHESTRATOR_MODE", "QUEUE_ASYNC");
        pb.environment().put("PIPELINE_ORCHESTRATOR_RESUME_TOKEN_SECRET", E2E_RESUME_TOKEN_SECRET);
        pb.environment().put("PIPELINE_ORCHESTRATOR_IDEMPOTENCY_POLICY", "CLIENT_KEY_REQUIRED");
        pb.environment().put("KAFKA_BOOTSTRAP_SERVERS", getKafkaContainer().getBootstrapServers());
        String runId = UUID.randomUUID().toString();
        pb.environment().put(AWAIT_RESPONSES_GROUP_ENV, "csv-payments-orchestrator-" + runId);
        // Use earliest with per-run consumer groups so the run cannot miss completions published
        // before the consumer assignment settles. Stale history from prior runs is tolerated and
        // deterministically dropped by await completion admission.
        pb.environment().put(AWAIT_RESPONSES_OFFSET_RESET_ENV, "earliest");
        if (MONOLITH_LAYOUT) {
            pb.environment().put(PROVIDER_REQUESTS_GROUP_ENV, "csv-payments-mock-provider-" + runId);
            pb.environment().put(PROVIDER_REQUESTS_OFFSET_RESET_ENV, "earliest");
        }
        configureObservabilityProcessEnv(pb.environment());

        if (MONOLITH_LAYOUT) {
            configureMonolithEnv(pb);
        } else if (PIPELINE_RUNTIME_LAYOUT) {
            configurePipelineRuntimeEnv(pb);
        } else {
            configureModularEnv(pb);
        }
        pb.environment().putAll(envOverrides);

        pb.redirectErrorStream(true);
        Process p = pb.start();
        StringBuilder processOutput = new StringBuilder();
        Thread outputDrainer =
            new Thread(
                () -> {
                    try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            processOutput.append(line).append(System.lineSeparator());
                            LOG.infof("[orchestrator] %s", line);
                        }
                    } catch (IOException e) {
                        if (!p.isAlive() && isExpectedClosedStream(e)) {
                            LOG.debug("Orchestrator process output stream closed after process exit.");
                        } else {
                            LOG.warnf(e, "Failed reading orchestrator process output.");
                        }
                    }
                },
                "orchestrator-output-drainer");
        outputDrainer.setDaemon(true);
        outputDrainer.start();

        boolean completed = p.waitFor(ORCHESTRATOR_WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (!completed) {
            try {
                p.destroy();
                if (!p.waitFor(5, TimeUnit.SECONDS)) {
                    p.destroyForcibly();
                    p.waitFor(5, TimeUnit.SECONDS);
                }
            } catch (InterruptedException ie) {
                p.destroyForcibly();
                Thread.currentThread().interrupt();
            }
            fail(
                    "Orchestrator process timed out after "
                            + ORCHESTRATOR_WAIT_TIMEOUT_SECONDS
                            + "s. Output tail:\n"
                            + tailLines(processOutput.toString(), 120));
        }
        outputDrainer.join(TimeUnit.SECONDS.toMillis(5));
        int exitCode = p.exitValue();
        String output = processOutput.toString();
        assertEquals(
            0,
            exitCode,
            "Orchestrator exited with non-zero code. Output tail:\n" + tailLines(output, 200));
        return new ProcessRunResult(exitCode, output);
    }

    /**
     * Configure the given ProcessBuilder's environment for running the orchestrator in a monolith layout.
     *
     * Sets up local pipeline transport, keystore/truststore paths, Hibernate and datasource settings pointing
     * to the test PostgreSQL container, a persistence provider, and gRPC client host/port entries that route
     * all internal service endpoints to the local orchestrator port.
     *
     * @param pb the ProcessBuilder whose environment will be modified for monolith execution
     */
    private static void configureMonolithEnv(ProcessBuilder pb) {
        PostgreSQLContainer<?> postgres = getPostgresContainer();
        pb.environment().put("PIPELINE_TRANSPORT", "LOCAL");
        pb.environment()
                .put(
                        "SERVER_KEYSTORE_PATH",
                        DEV_CERTS_DIR.resolve("orchestrator-svc/server-keystore.jks").toString());
        pb.environment()
                .put(
                        "CLIENT_TRUSTSTORE_PATH",
                        DEV_CERTS_DIR.resolve("orchestrator-svc/client-truststore.jks").toString());
        pb.environment().put("QUARKUS_GRPC_SERVER_USE_SEPARATE_SERVER", "false");

        String postgresUrl =
                "postgresql://"
                        + postgres.getHost()
                        + ":"
                        + postgres.getMappedPort(5432)
                        + "/quarkus";
        pb.environment().put("QUARKUS_DATASOURCE_JDBC_URL", "jdbc:" + postgresUrl);
        pb.environment()
                .put(
                        "QUARKUS_DATASOURCE_REACTIVE_URL",
                        "postgresql://"
                                + postgres.getHost()
                                + ":"
                                + postgres.getMappedPort(5432)
                                + "/quarkus");
        pb.environment()
                .put(
                        "PERSISTENCE_PROVIDER_CLASS",
                        "org.pipelineframework.plugin.persistence.provider.ReactivePanachePersistenceProvider");
        pb.environment().put("QUARKUS_HIBERNATE_ORM_BLOCKING", "false");
        pb.environment().put("QUARKUS_HIBERNATE_ORM_SCHEMA_MANAGEMENT_STRATEGY", "drop-and-create");
        pb.environment().put("QUARKUS_HIBERNATE_ORM_PACKAGES", "org.pipelineframework.csv.common.domain");
        pb.environment().put("QUARKUS_DATASOURCE_USERNAME", postgres.getUsername());
        pb.environment().put("QUARKUS_DATASOURCE_PASSWORD", postgres.getPassword());

        String localhost = "localhost";
        String grpcPort = "8443";
        putGrpcClient(pb, "PROCESS_CSV_PAYMENTS_INPUT", localhost, grpcPort);
        putGrpcClient(pb, "PROCESS_SEND_PAYMENT_RECORD", localhost, grpcPort);
        putGrpcClient(pb, "PROCESS_ACK_PAYMENT_SENT", localhost, grpcPort);
        putGrpcClient(pb, "PROCESS_APPROVED_PAYMENT_STATUS", localhost, grpcPort);
        putGrpcClient(pb, "PROCESS_UNAPPROVED_PAYMENT_STATUS", localhost, grpcPort);
        putGrpcClient(pb, "PROCESS_FINALIZE_PAYMENT_OUTPUT", localhost, grpcPort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_CSV_PAYMENTS_INPUT_FILE_SIDE_EFFECT", localhost, grpcPort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_PAYMENT_RECORD_SIDE_EFFECT", localhost, grpcPort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_ACK_PAYMENT_SENT_SIDE_EFFECT", localhost, grpcPort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_PAYMENT_STATUS_SIDE_EFFECT", localhost, grpcPort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_PAYMENT_OUTPUT_SIDE_EFFECT", localhost, grpcPort);
    }

    /**
     * Configure the given ProcessBuilder's environment so the orchestrator connects to modular service
     * containers over gRPC.
     *
     * <p>Sets QUARKUS gRPC client host and port environment variables for input, payments processing,
     * payment status, output file processing, and persistence services.
     *
     * @param pb the ProcessBuilder whose environment will be populated with gRPC client host/port entries
     */
    private static void configureModularEnv(ProcessBuilder pb) {
        GenericContainer<?> inputService = getInputCsvService();
        GenericContainer<?> paymentsService = getPaymentsProcessingService();
        GenericContainer<?> statusService = getPaymentStatusService();
        GenericContainer<?> persistence = getPersistenceService();
        pb.environment()
            .put(
                "SERVER_KEYSTORE_PATH",
                DEV_CERTS_DIR.resolve("orchestrator-svc/server-keystore.jks").toString());
        pb.environment()
            .put(
                "CLIENT_TRUSTSTORE_PATH",
                DEV_CERTS_DIR.resolve("orchestrator-svc/client-truststore.jks").toString());
        putGrpcClient(pb, "PROCESS_CSV_PAYMENTS_INPUT", inputService.getHost(), String.valueOf(inputService.getMappedPort(8444)), "/q/health/live");
        putGrpcClient(pb, "PROCESS_SEND_PAYMENT_RECORD", paymentsService.getHost(), String.valueOf(paymentsService.getMappedPort(8445)), "/q/health/live");
        putGrpcClient(pb, "PROCESS_ACK_PAYMENT_SENT", paymentsService.getHost(), String.valueOf(paymentsService.getMappedPort(8445)), "/q/health/live");
        putGrpcClient(pb, "PROCESS_APPROVED_PAYMENT_STATUS", statusService.getHost(), String.valueOf(statusService.getMappedPort(8446)), "/q/health/live");
        putGrpcClient(pb, "PROCESS_UNAPPROVED_PAYMENT_STATUS", statusService.getHost(), String.valueOf(statusService.getMappedPort(8446)), "/q/health/live");
        putGrpcClient(pb, "PROCESS_FINALIZE_PAYMENT_OUTPUT", statusService.getHost(), String.valueOf(statusService.getMappedPort(8446)), "/q/health/live");
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_CSV_PAYMENTS_INPUT_FILE_SIDE_EFFECT", persistence.getHost(), String.valueOf(persistence.getMappedPort(8448)), "/q/health/live");
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_PAYMENT_RECORD_SIDE_EFFECT", persistence.getHost(), String.valueOf(persistence.getMappedPort(8448)), "/q/health/live");
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_ACK_PAYMENT_SENT_SIDE_EFFECT", persistence.getHost(), String.valueOf(persistence.getMappedPort(8448)), "/q/health/live");
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_PAYMENT_STATUS_SIDE_EFFECT", persistence.getHost(), String.valueOf(persistence.getMappedPort(8448)), "/q/health/live");
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_PAYMENT_OUTPUT_SIDE_EFFECT", persistence.getHost(), String.valueOf(persistence.getMappedPort(8448)), "/q/health/live");
    }

    /**
     * Configure the given ProcessBuilder's environment for pipeline-runtime layout (grouped pipeline runtime plus
     * standalone persistence runtime).
     *
     * @param pb the ProcessBuilder whose environment will be populated
     */
    private static void configurePipelineRuntimeEnv(ProcessBuilder pb) {
        GenericContainer<?> pipelineService = getPipelineRuntimeService();
        GenericContainer<?> persistence = getPersistenceService();
        pb.environment()
            .put(
                "SERVER_KEYSTORE_PATH",
                DEV_CERTS_DIR.resolve("orchestrator-svc/server-keystore.jks").toString());
        pb.environment()
            .put(
                "CLIENT_TRUSTSTORE_PATH",
                DEV_CERTS_DIR.resolve("orchestrator-svc/client-truststore.jks").toString());
        String pipelineHost = pipelineService.getHost();
        String pipelinePort = String.valueOf(pipelineService.getMappedPort(8445));
        String persistenceHost = persistence.getHost();
        String persistencePort = String.valueOf(persistence.getMappedPort(8448));

        putGrpcClient(pb, "PROCESS_CSV_PAYMENTS_INPUT", pipelineHost, pipelinePort);
        putGrpcClient(pb, "PROCESS_SEND_PAYMENT_RECORD", pipelineHost, pipelinePort);
        putGrpcClient(pb, "PROCESS_ACK_PAYMENT_SENT", pipelineHost, pipelinePort);
        putGrpcClient(pb, "PROCESS_APPROVED_PAYMENT_STATUS", pipelineHost, pipelinePort);
        putGrpcClient(pb, "PROCESS_UNAPPROVED_PAYMENT_STATUS", pipelineHost, pipelinePort);
        putGrpcClient(pb, "PROCESS_FINALIZE_PAYMENT_OUTPUT", pipelineHost, pipelinePort);

        putGrpcClient(pb, "OBSERVE_PERSISTENCE_CSV_PAYMENTS_INPUT_FILE_SIDE_EFFECT", persistenceHost, persistencePort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_PAYMENT_RECORD_SIDE_EFFECT", persistenceHost, persistencePort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_ACK_PAYMENT_SENT_SIDE_EFFECT", persistenceHost, persistencePort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_PAYMENT_STATUS_SIDE_EFFECT", persistenceHost, persistencePort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_PAYMENT_OUTPUT_SIDE_EFFECT", persistenceHost, persistencePort);
    }

    private static Path writeE2ePipelineConfig() throws IOException {
        Path config = Paths.get(TEST_E2E_DIR, "pipeline-e2e.yaml");
        Path canonical = Paths.get(System.getProperty("user.dir"))
                .resolve("../config/pipeline.yaml")
                .normalize()
                .toAbsolutePath();
        String root = Paths.get(TEST_E2E_DIR).toAbsolutePath().normalize().toString();
        Map<String, Object> yaml = loadYamlMap(canonical);
        Map<String, Object> source = childMap(childMap(yaml, "sources"), "csv-payment-files");
        childMap(source, "location").put("root", root);
        childMap(source, "poll").put("enabled", false);

        Map<String, Object> publish = childMap(childMap(yaml, "publish"), "csv-payment-output-files");
        childMap(publish, "location").put("root", root);

        if (!MONOLITH_LAYOUT) {
            childMap(source, "location").put("localPathRoot", TEST_E2E_TARGET_DIR);
        }
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(true);
        Files.writeString(config, new Yaml(options).dump(yaml), StandardCharsets.UTF_8);
        return config.toAbsolutePath().normalize();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> loadYamlMap(Path path) throws IOException {
        Object loaded;
        try (var reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            loaded = new Yaml().load(reader);
        }
        if (loaded instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        throw new IllegalStateException("Expected YAML object in " + path);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> childMap(Map<String, Object> parent, String key) {
        Object child = parent.get(key);
        if (child instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        throw new IllegalStateException("Expected YAML object at key '" + key + "'");
    }

    private static void ensureKafkaTopics() {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        Throwable lastFailure = null;
        int attempt = 0;

        while (System.nanoTime() < deadline) {
            attempt++;
            Map<String, Object> config = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                getKafkaContainer().getBootstrapServers(),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                "10000",
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                "15000");
            try (AdminClient admin = AdminClient.create(config)) {
                admin.createTopics(List.of(
                    new NewTopic(PAYMENT_REQUEST_TOPIC, 1, (short) 1),
                    new NewTopic(PAYMENT_RESULT_TOPIC, 1, (short) 1)))
                    .all()
                    .get(15, TimeUnit.SECONDS);
                return;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    return;
                }
                lastFailure = e;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Failed to create Kafka topics for CSV payments E2E", e);
            } catch (Exception e) {
                lastFailure = e;
            }

            if (System.nanoTime() < deadline) {
                LOG.warnf(lastFailure, "Kafka topic creation attempt %d failed; retrying.", attempt);
                try {
                    Thread.sleep(2_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Failed to create Kafka topics for CSV payments E2E", e);
                }
            }
        }
        throw new IllegalStateException("Failed to create Kafka topics for CSV payments E2E", lastFailure);
    }

    private static boolean isExpectedClosedStream(IOException e) {
        String message = e.getMessage();
        return message != null && message.contains("Stream closed");
    }

    /**
     * Adds QUARKUS gRPC client host and port environment variables to the given ProcessBuilder.
     *
     * @param pb         the ProcessBuilder whose environment will be modified
     * @param clientName the client identifier used in the environment variable names (e.g. "PAYMENTS_SVC")
     * @param host       the host value to assign to the client's HOST environment variable
     * @param port       the port value to assign to the client's PORT environment variable
     */
    private static void putGrpcClient(ProcessBuilder pb, String clientName, String host, String port) {
        pb.environment().put("QUARKUS_GRPC_CLIENTS_" + clientName + "_HOST", host);
        pb.environment().put("QUARKUS_GRPC_CLIENTS_" + clientName + "_PORT", port);
    }

    private static void putGrpcClient(
            ProcessBuilder pb, String clientName, String host, String port, String healthPath) {
        putGrpcClient(pb, clientName, host, port);
        pb.environment().put("QUARKUS_GRPC_CLIENTS_" + clientName + "_HEALTH_PATH", healthPath);
    }

    /**
     * Resolve the filesystem path to the orchestrator JAR, locating it from several likely locations.
     *
     * If `jarPath` points to an existing regular file that path is returned. If `jarPath` is relative,
     * it is resolved against the current working directory and returned if present. When `monolithLayout`
     * is true (or the provided path suggests a monolith layout), the method will also search upward from
     * the working directory for the repository's monolith JAR location.
     *
     * @param jarPath the original JAR path candidate (absolute or relative)
     * @param monolithLayout true to prefer/allow searching for the monolith JAR location in parent directories
     * @return a Path pointing to a discovered JAR file when found, otherwise the normalized input candidate
     */
    private static Path resolveJarPath(String jarPath, boolean monolithLayout) {
        Path candidate = Paths.get(jarPath).normalize();
        if (Files.isRegularFile(candidate)) {
            return candidate;
        }
        if (!candidate.isAbsolute()) {
            Path cwd = Paths.get(System.getProperty("user.dir")).normalize();
            Path resolved = cwd.resolve(jarPath).normalize();
            if (Files.isRegularFile(resolved)) {
                return resolved;
            }
            if (monolithLayout || jarPath.contains("monolith-svc/target/quarkus-app/quarkus-run.jar")) {
                Path cursor = cwd;
                for (int i = 0; i < 6 && cursor != null; i++) {
                    Path repoCandidate = cursor
                            .resolve("examples/csv-payments/monolith-svc/target/quarkus-app/quarkus-run.jar")
                            .normalize();
                    if (Files.isRegularFile(repoCandidate)) {
                        return repoCandidate;
                    }
                    cursor = cursor.getParent();
                }
            }
        }
        return candidate;
    }

    /**
     * Remove any existing "*.csv" and "*.out" files from the given test output directory.
     *
     * @param outputDir the path to the test output directory to clean
     * @throws IOException if an I/O error occurs while accessing or listing the directory
     */
    private void cleanTestOutputDirectory(Path outputDir) throws IOException {
        // Delete any existing CSV and OUT files in the test output directory
        if (Files.exists(outputDir)) {
            try (var files = Files.list(outputDir)) {
                files.filter(
                                path ->
                                        path.toString().endsWith(".csv")
                                                || path.toString().endsWith(".out"))
                        .forEach(
                                path -> {
                                    try {
                                        Files.deleteIfExists(path);
                                        LOG.infof("Deleted existing file: %s", path);
                                    } catch (IOException e) {
                                        LOG.warnf(e, "Failed to delete existing file: %s", path);
                                    }
                                });
            }
        }
    }

    /**
     * Creates two CSV files under the test directory containing five payment records for the end-to-end test.
     *
     * <p>The files are "payments_first.csv" (three records) and "payments_second.csv" (two records). The method also
     * makes the files writable and logs the created CSV filenames.
     *
     * @throws IOException if an I/O error occurs while writing the files or listing the directory
     */
    private void createTestCsvFiles() throws IOException {
        if (CUSTOM_INPUT_FILE) {
            createCustomInputCsvFile();
            return;
        }

        LOG.info("Creating test CSV files...");

        // Create first test file with 3 records
        Path firstFile = Paths.get(TEST_E2E_DIR, "payments_first.csv");
        Files.write(
                firstFile,
                """
            ID,Recipient,Amount,Currency
            1,John Doe,100.00,USD
            2,Jane Smith,200.00,EUR
            3,Bob Johnson,300.00,GBP
            """
                        .getBytes(StandardCharsets.UTF_8));

        // Create second test file with 2 records
        Path secondFile = Paths.get(TEST_E2E_DIR, "payments_second.csv");
        Files.write(
                secondFile,
                """
            ID,Recipient,Amount,Currency
            1,Alice Brown,150.00,AUD
            2,Charlie Wilson,250.00,CAD
            """
                        .getBytes(StandardCharsets.UTF_8));

        ensureWritable(firstFile);
        ensureWritable(secondFile);

        LOG.info("Created test CSV files:");
        try (var files = Files.list(Paths.get(TEST_E2E_DIR))) {
            files.filter(path -> path.toString().endsWith(".csv"))
                    .forEach(path -> LOG.infof("- %s", path));
        }
    }

    private void createCustomInputCsvFile() throws IOException {
        Path source = resolveCustomInputCsvFile();
        assertTrue(Files.isRegularFile(source), "Expected custom CSV input file at " + source);

        String fileName = source.getFileName().toString();
        if (fileName.endsWith(".skip")) {
            fileName = fileName.substring(0, fileName.length() - ".skip".length());
        }
        if (!fileName.endsWith(".csv")) {
            fileName = fileName + ".csv";
        }

        Path target = Paths.get(TEST_E2E_DIR, fileName);
        Files.copy(source, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        ensureWritable(target);

        LOG.infof(
                "Created custom E2E CSV input file %s from %s with %,d expected records.",
                target,
                source,
                expectedPaymentRecordCount());
    }

    private Path resolveCustomInputCsvFile() {
        Path configured = Paths.get(CSV_E2E_INPUT_FILE);
        if (configured.isAbsolute()) {
            return configured.normalize();
        }

        Path current = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
        while (current != null) {
            Path candidate = current.resolve(configured).normalize();
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            current = current.getParent();
        }
        return configured.toAbsolutePath().normalize();
    }

    private void createValidAndMalformedCsvFiles() throws IOException {
        LOG.info("Creating mixed valid/malformed CSV files...");

        Path validFile = Paths.get(TEST_E2E_DIR, "payments_valid.csv");
        Files.write(
            validFile,
            """
            ID,Recipient,Amount,Currency
            1,Valid Recipient One,125.00,USD
            2,Valid Recipient Two,275.50,EUR
            """
                .getBytes(StandardCharsets.UTF_8));

        Path malformedFile = Paths.get(TEST_E2E_DIR, "payments_malformed.csv");
        Files.write(
            malformedFile,
            """
            ID,Recipient,Amount,Currency
            9,Malformed Recipient,"BROKEN,USD
            """
                .getBytes(StandardCharsets.UTF_8));

        ensureWritable(validFile);
        ensureWritable(malformedFile);

        LOG.infof("Created reject-scenario files: %s, %s", validFile, malformedFile);
    }

    /**
     * Waits until at least one pipeline output file (a file ending with ".out") appears in the test
     * output directory or a configurable timeout elapses.
     *
     * <p>Polls the TEST_E2E_DIR and returns as soon as any ".out" file is detected; if the timeout is
     * reached without finding output files the test is failed.
     *
     * <p>Timeout can be tuned via system property {@code csv.e2e.pipeline.wait.seconds}; default is 60
     * seconds.
     *
     * @throws InterruptedException if the thread is interrupted while sleeping between polls
     * @throws IOException if an I/O error occurs when listing the test directory
     */
    @SuppressWarnings("BusyWait")
    private void waitForPipelineComplete() throws InterruptedException, IOException {
        LOG.info("Waiting for pipeline to complete processing...");

        // Check for output files to be created before continuing
        long startTime = System.currentTimeMillis();
        long timeout = TimeUnit.SECONDS.toMillis(PIPELINE_WAIT_TIMEOUT_SECONDS);
        int pollCount = 0;

        while (System.currentTimeMillis() - startTime < timeout) {
            // Check if output files exist in the expected output directory
            boolean outputFilesExist;
            try (var files = Files.list(Paths.get(TEST_E2E_DIR))) {
                outputFilesExist = files.anyMatch(path -> path.toString().endsWith(".out"));
            }

            if (outputFilesExist && outputRecordCountReady()) {
                LOG.info("Output files detected, pipeline processing completed");
                return;
            }

            pollCount++;
            if (pollCount % 5 == 0) {
                try (var files = Files.list(Paths.get(TEST_E2E_DIR))) {
                    long csvFiles = files.filter(path -> path.toString().endsWith(".csv")).count();
                    LOG.infof(
                            "Still waiting for .out files in %s (elapsed=%ds, csvFiles=%d)",
                            TEST_E2E_DIR,
                            TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime),
                            csvFiles);
                }
            }

            Thread.sleep(PIPELINE_WAIT_POLL_MILLIS);
        }

        logPipelineTimeoutDiagnostics(startTime);
        fail(
                "Pipeline completion timeout reached with no .out files in "
                        + TEST_E2E_DIR
                        + " after "
                        + PIPELINE_WAIT_TIMEOUT_SECONDS
                        + "s");
    }

    private boolean outputRecordCountReady() throws IOException {
        long currentRecords = outputRecordCount(Paths.get(TEST_E2E_DIR));
        long expectedRecords = expectedOutputRecordCount();
        if (currentRecords >= expectedRecords) {
            LOG.infof(
                    "Output record count reached expected count: %,d/%s",
                    currentRecords,
                    expectedRecords);
            return true;
        }
        LOG.infof("Waiting for output records: %,d/%,d", currentRecords, expectedRecords);
        return false;
    }

    private long expectedOutputRecordCount() throws IOException {
        return expectedPaymentRecordCount();
    }

    private long expectedProviderRejectCount(double rejectProbability) throws IOException {
        if (!CUSTOM_INPUT_FILE) {
            return 0L;
        }
        try (Stream<String> lines = Files.lines(resolveCustomInputCsvFile())) {
            return lines.skip(1)
                    .map(String::trim)
                    .filter(line -> !line.isBlank())
                    .map(line -> line.split(",", 2))
                    .filter(columns -> columns.length > 0 && !columns[0].isBlank())
                    .map(columns -> columns[0].trim())
                    .filter(csvId -> shouldSimulateProviderReject(rejectProbability, csvId))
                    .count();
        }
    }

    private static boolean shouldSimulateProviderReject(double probability, String simulationKey) {
        if (probability <= 0.0d) {
            return false;
        }
        if (probability >= 1.0d) {
            return true;
        }
        long normalized = Integer.toUnsignedLong(java.util.Objects.hash(simulationKey, "provider-reject"));
        long threshold = Math.round(probability * 10_000d);
        return normalized % 10_000L < threshold;
    }

    private void logPipelineTimeoutDiagnostics(long startTimeMillis) {
        long elapsedSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTimeMillis);
        LOG.errorf(
                "Pipeline timeout diagnostics (layout=%s, elapsed=%ds, timeout=%ds, outputDir=%s)",
                RUNTIME_LAYOUT,
                elapsedSeconds,
                PIPELINE_WAIT_TIMEOUT_SECONDS,
                TEST_E2E_DIR);
        logTestE2EDirectorySnapshot();
        logContainerLogTail("kafka", kafkaContainer, 160);
        logContainerLogTail("payments-processing-svc", paymentsProcessingService, 200);
        logContainerLogTail("pipeline-runtime-svc", pipelineRuntimeService, 200);
        logContainerLogTail("persistence-svc", persistenceService, 200);
        logContainerLogTail("postgres", postgresContainer, 120);
    }

    private void logTestE2EDirectorySnapshot() {
        Path dir = Paths.get(TEST_E2E_DIR);
        if (!Files.exists(dir)) {
            LOG.errorf("Timeout diagnostics: output directory does not exist: %s", dir);
            return;
        }
        try (var files = Files.list(dir).sorted()) {
            List<Path> paths = files.toList();
            if (paths.isEmpty()) {
                LOG.errorf("Timeout diagnostics: output directory is empty: %s", dir);
                return;
            }
            for (Path path : paths) {
                try {
                    LOG.errorf(
                            "Timeout diagnostics: file=%s size=%d modified=%s",
                            path.getFileName(),
                            Files.size(path),
                            Files.getLastModifiedTime(path));
                } catch (IOException e) {
                    LOG.errorf(e, "Timeout diagnostics: failed to read file metadata for %s", path);
                }
            }
        } catch (IOException e) {
            LOG.errorf(e, "Timeout diagnostics: failed to list output directory %s", dir);
        }
    }

    private static void logContainerLogTail(String containerName, GenericContainer<?> container, int maxLines) {
        if (container == null) {
            LOG.errorf("Timeout diagnostics: container %s was not initialized", containerName);
            return;
        }
        try {
            String logs = container.getLogs();
            if (logs == null || logs.isBlank()) {
                LOG.errorf("Timeout diagnostics: no logs available for %s", containerName);
                return;
            }
            LOG.errorf(
                    "Timeout diagnostics: last %d log lines for %s:%n%s",
                    maxLines,
                    containerName,
                    tailLines(logs, maxLines));
        } catch (Exception e) {
            LOG.errorf(e, "Timeout diagnostics: failed to read logs for %s", containerName);
        }
    }

    private static String tailLines(String text, int maxLines) {
        String[] lines = text.split("\\R");
        if (lines.length <= maxLines) {
            return text;
        }
        int from = lines.length - maxLines;
        StringBuilder sb = new StringBuilder();
        for (int i = from; i < lines.length; i++) {
            sb.append(lines[i]).append(System.lineSeparator());
        }
        return sb.toString();
    }

    /**
         * Validates that output `.out` files in the specified directory contain the expected payment records.
         *
         * <p>Checks that at least one `.out` file exists, that the combined number of data records
         * (excluding header lines) is at least five, and that records for the recipients
         * John Doe, Jane Smith, Bob Johnson, Alice Brown, and Charlie Wilson are present.
         *
         * @param testOutputTargetDir path to the directory containing generated output files
         * @throws IOException if an I/O error occurs while listing or reading output files
         */
    @SuppressWarnings("SameParameterValue")
    private void verifyOutputFiles(String testOutputTargetDir) throws IOException {
        LOG.info("Verifying output files...");

        // Check if output files exist in the output directory
        List<Path> outputFiles;
        try (var files = Files.list(Paths.get(testOutputTargetDir))) {
            outputFiles = files.filter(path -> path.toString().endsWith(".out")).toList();
        }

        // Output files should be generated
        assertFalse(outputFiles.isEmpty(), "Output files should be generated");

        LOG.info("Found output files: " + outputFiles);

        long totalRecords = outputRecordCount(Paths.get(testOutputTargetDir));

        LOG.infof("Total records across all output files: %d", totalRecords);

        long expectedRecords = expectedPaymentRecordCount();
        assertTrue(
                totalRecords >= expectedRecords,
                String.format("Expected at least %d records, but found %d", expectedRecords, totalRecords));

        if (CUSTOM_INPUT_FILE) {
            LOG.info("Custom input output record count verified");
            return;
        }

        Set<String> expectedRecipients = Set.of(
            "John Doe", "Jane Smith", "Bob Johnson", "Alice Brown", "Charlie Wilson");
        Set<String> foundRecipients = new HashSet<>();
        for (Path outputFile : outputFiles) {
            String content = Files.readString(outputFile);
            expectedRecipients.stream()
                .filter(content::contains)
                .forEach(foundRecipients::add);
        }
        assertTrue(foundRecipients.containsAll(expectedRecipients),
            "Missing expected recipients in output files: " + expectedRecipients.stream()
                .filter(recipient -> !foundRecipients.contains(recipient))
                .toList());

        LOG.info("All expected records found in output files");
    }

    private void verifyOutputFilesForRecipients(
        String testOutputTargetDir,
        Set<String> expectedRecipients,
        String unexpectedRecipient,
        long expectedTotalRecords
    ) throws IOException {
        LOG.info("Verifying output files for reject scenario...");

        List<Path> outputFiles;
        try (var files = Files.list(Paths.get(testOutputTargetDir))) {
            outputFiles = files.filter(path -> path.toString().endsWith(".out")).toList();
        }
        assertFalse(outputFiles.isEmpty(), "Output files should be generated");

        long totalRecords = outputFiles.stream()
            .mapToLong(path -> {
                try {
                    List<String> lines = Files.readAllLines(path);
                    return Math.max(0, lines.size() - 1);
                } catch (IOException e) {
                    LOG.warnf(e, "Failed to read file: %s", path);
                    return 0L;
                }
            })
            .sum();

        assertEquals(
            expectedTotalRecords,
            totalRecords,
            String.format("Expected %d records across output files, but found %d", expectedTotalRecords, totalRecords));

        String combinedOutput = outputFiles.stream()
            .map(path -> {
                try {
                    return Files.readString(path);
                } catch (IOException e) {
                    LOG.warnf(e, "Failed reading output file: %s", path);
                    return "";
                }
            })
            .reduce("", String::concat);

        for (String recipient : expectedRecipients) {
            assertTrue(
                combinedOutput.contains(recipient),
                "Expected recipient not found in output files: " + recipient);
        }
        assertFalse(
            combinedOutput.contains(unexpectedRecipient),
            "Malformed recipient should not be written to output files: " + unexpectedRecipient);
    }

    private long outputRecordCount(Path outputDir) throws IOException {
        try (var files = Files.list(outputDir)) {
            return files.filter(path -> path.toString().endsWith(".out"))
                    .mapToLong(
                            path -> {
                                try (Stream<String> lines = Files.lines(path)) {
                                    return Math.max(0L, lines.count() - 1L);
                                } catch (IOException e) {
                                    LOG.warnf(e, "Failed to read file: %s", path);
                                    return 0L;
                                }
                            })
                    .sum();
        }
    }

    private String readCombinedOutput(String outputDir) throws IOException {
        try (var files = Files.list(Paths.get(outputDir))) {
            return files.filter(path -> path.toString().endsWith(".out"))
                    .map(
                            path -> {
                                try {
                                    return Files.readString(path);
                                } catch (IOException e) {
                                    LOG.warnf(e, "Failed reading output file: %s", path);
                                    return "";
                                }
                            })
                    .reduce("", String::concat);
        }
    }

    private long expectedPaymentRecordCount() throws IOException {
        if (!CUSTOM_INPUT_FILE) {
            return 5L;
        }
        try (Stream<String> lines = Files.lines(resolveCustomInputCsvFile())) {
            return Math.max(0L, lines.count() - 1L);
        }
    }

    private static double configuredProviderRejectProbability() {
        try {
            return Double.parseDouble(System.getProperty("csv-payments.payment-provider.provider-reject-probability", "0"));
        } catch (NumberFormatException ignored) {
            return 0.0d;
        }
    }

    private void assertTempoTracesAvailable() throws Exception {
        JsonNode searchResponse = waitForTempoTraceSearch(List.of(
                "{ name = \"tpf.pipeline.run\" }",
                "{ name = \"tpf.step\" }"));
        Set<String> traceIds = extractTraceIds(searchResponse);
        assertFalse(
                traceIds.isEmpty(),
                "Expected Tempo search to return at least one trace. Search response: " + searchResponse);

        for (String traceId : traceIds) {
            JsonNode traceDocument;
            try {
                traceDocument = fetchTempoTrace(traceId);
            } catch (Exception e) {
                LOG.warnf(e, "Unable to fetch Tempo trace %s during verification.", traceId);
                continue;
            }
            if (containsTpfTraceSemantics(traceDocument)) {
                LOG.infof("Verified Tempo trace %s contains TPF spans.", traceId);
                return;
            }
        }

        fail(
                "Tempo returned traces, but none contained TPF span names. Trace IDs: "
                        + traceIds
                        + ", search response: "
                        + searchResponse);
    }

    private JsonNode waitForTempoTraceSearch(List<String> candidateQueries) throws Exception {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(TEMPO_SEARCH_TIMEOUT_SECONDS);
        JsonNode lastResponse = null;

        while (System.currentTimeMillis() < deadline) {
            for (String query : candidateQueries) {
                JsonNode response;
                try {
                    response = searchTempo(query);
                } catch (Exception e) {
                    LOG.debugf(e, "Tempo search query failed during warm-up: %s", query);
                    lastResponse = OBJECT_MAPPER.getNodeFactory().nullNode();
                    continue;
                }
                lastResponse = response;
                if (!extractTraceIds(response).isEmpty()) {
                    return response;
                }
            }
            Thread.sleep(TEMPO_SEARCH_POLL_MILLIS);
        }

        JsonNode serviceNames = tempoServiceNameValues();
        fail(
                "Timed out waiting for Tempo traces after "
                        + TEMPO_SEARCH_TIMEOUT_SECONDS
                        + "s. Last search response: "
                        + lastResponse
                        + ". Service names visible in Tempo: "
                        + serviceNames);
        return OBJECT_MAPPER.getNodeFactory().nullNode();
    }

    private boolean containsTpfTraceSemantics(JsonNode traceDocument) {
        List<String> spanNames = traceDocument.findValuesAsText("name");
        if (spanNames.contains("tpf.pipeline.run") || spanNames.contains("tpf.step")) {
            return true;
        }

        List<String> attributeKeys = traceDocument.findValuesAsText("key");
        return attributeKeys.contains("tpf.pipeline")
                || attributeKeys.contains("tpf.step")
                || attributeKeys.contains("tpf.step.class")
                || traceDocument.toString().contains("\"tpf.pipeline.run\"")
                || traceDocument.toString().contains("\"tpf.step\"");
    }

    private JsonNode searchTempo(String traceQl) throws Exception {
        String encoded = URLEncoder.encode(traceQl, StandardCharsets.UTF_8);
        URI uri = URI.create(tempoApiBaseUrl() + "/api/search?q=" + encoded + "&limit=20");
        return httpGetJson(uri);
    }

    private JsonNode tempoServiceNameValues() {
        try {
            return httpGetJson(URI.create(tempoApiBaseUrl() + "/api/search/tag/service.name/values"));
        } catch (Exception e) {
            LOG.warnf(e, "Unable to query Tempo service.name values.");
            return OBJECT_MAPPER.getNodeFactory().nullNode();
        }
    }

    private JsonNode fetchTempoTrace(String traceId) throws Exception {
        Exception v2Failure = null;
        try {
            return httpGetJson(URI.create(tempoApiBaseUrl() + "/api/v2/traces/" + traceId));
        } catch (Exception e) {
            v2Failure = e;
        }
        try {
            return httpGetJson(URI.create(tempoApiBaseUrl() + "/api/traces/" + traceId));
        } catch (Exception e) {
            if (v2Failure != null) {
                e.addSuppressed(v2Failure);
            }
            throw e;
        }
    }

    private JsonNode httpGetJson(URI uri) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(TEMPO_HTTP_REQUEST_TIMEOUT)
                .GET()
                .build();
        HttpResponse<String> response =
                HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        assertEquals(
                200,
                response.statusCode(),
                "Expected HTTP 200 from " + uri + " but got " + response.statusCode() + ". Body: " + response.body());
        return OBJECT_MAPPER.readTree(response.body());
    }

    private Set<String> extractTraceIds(JsonNode searchResponse) {
        Set<String> traceIds = new HashSet<>();
        traceIds.addAll(searchResponse.findValuesAsText("traceID"));
        traceIds.addAll(searchResponse.findValuesAsText("traceId"));
        traceIds.removeIf(String::isBlank);
        return traceIds;
    }

    /**
     * Verify that the expected payment records were persisted to the test PostgreSQL database.
     *
     * <p>Performs the following checks against the test container database: - the `paymentrecord`
     * table exists, - the table contains exactly five records, - specific recipient records are
     * present.
     *
     * @throws Exception if a JDBC driver or connection error occurs or verification cannot be
     *     performed
     */
    private void verifyDatabasePersistence() throws Exception {
        LOG.info("Verifying database persistence...");

        // Connect to the database using the test container's connection details
        PostgreSQLContainer<?> postgres = getPostgresContainer();
        String jdbcUrl = postgres.getJdbcUrl();
        String username = postgres.getUsername();
        String password = postgres.getPassword();

        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            // Verify that the paymentrecord table exists
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet tables =
                    metaData.getTables(null, null, "paymentrecord", new String[] {"TABLE"})) {
                assertTrue(tables.next(), "paymentrecord table should exist in the database");
            }

            // Query the paymentrecord table to ensure records were persisted
            String query = "SELECT COUNT(*) FROM paymentrecord";
            try (PreparedStatement stmt = connection.prepareStatement(query);
                    ResultSet rs = stmt.executeQuery()) {

                assertTrue(rs.next(), "Query should return results");
                int recordCount = rs.getInt(1);

                LOG.infof("Found %d records in paymentrecord table", recordCount);

                long expectedRecords = expectedPaymentRecordCount();
                assertEquals(
                        expectedRecords,
                        recordCount,
                        String.format("Expected %d records in database, but found %d", expectedRecords, recordCount));
            }

            if (!CUSTOM_INPUT_FILE) {
                // Verify specific records exist in the database
                verifySpecificRecordsInDatabase(connection);
            }

            LOG.info("Database verification completed successfully");
        }
    }

    private void verifyDatabasePersistenceForRecipients(
        Set<String> expectedRecipients,
        String unexpectedRecipient,
        int expectedRecordCount
    ) throws Exception {
        LOG.info("Verifying database persistence for reject scenario...");

        PostgreSQLContainer<?> postgres = getPostgresContainer();
        String jdbcUrl = postgres.getJdbcUrl();
        String username = postgres.getUsername();
        String password = postgres.getPassword();

        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            String countQuery = "SELECT COUNT(*) FROM paymentrecord";
            try (PreparedStatement stmt = connection.prepareStatement(countQuery);
                ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next(), "Record count query should return a row");
                int recordCount = rs.getInt(1);
                assertEquals(
                    expectedRecordCount,
                    recordCount,
                    String.format("Expected %d records in paymentrecord, but found %d", expectedRecordCount, recordCount));
            }

            for (String recipient : expectedRecipients) {
                assertTrue(recipientExists(connection, recipient), "Expected recipient missing in DB: " + recipient);
            }
            assertFalse(
                recipientExists(connection, unexpectedRecipient),
                "Malformed recipient should not be persisted: " + unexpectedRecipient);
        }
    }

    private boolean recipientExists(Connection connection, String recipient) throws SQLException {
        String query = "SELECT COUNT(*) FROM paymentrecord WHERE recipient = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, recipient);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    private void resetDatabasePersistence() throws Exception {
        PostgreSQLContainer<?> postgres = getPostgresContainer();
        String jdbcUrl = postgres.getJdbcUrl();
        String username = postgres.getUsername();
        String password = postgres.getPassword();

        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
            PreparedStatement truncate = connection.prepareStatement("TRUNCATE TABLE paymentrecord CASCADE")) {
            truncate.execute();
        } catch (SQLException e) {
            LOG.warnf(e, "Failed truncating paymentrecord; continuing with current persistence state.");
        }
    }

    private record ProcessRunResult(int exitCode, String output) {}

    /**
     * Verifies that a payment record exists for each expected recipient in the database.
     *
     * Checks that the `paymentrecord` table contains at least one row for each predefined
     * recipient name and fails the test if any expected record is missing.
     *
     * @param connection a JDBC connection to the database containing the `paymentrecord` table
     * @throws SQLException if a database access error occurs while querying for records
     */
    private void verifySpecificRecordsInDatabase(Connection connection) throws SQLException {
        LOG.info("Verifying specific records in database...");

        // Check for records from first file
        String[] expectedRecords = {
            "John Doe", "Jane Smith", "Bob Johnson", "Alice Brown", "Charlie Wilson"
        };

        for (String recordName : expectedRecords) {
            String query = "SELECT COUNT(*) FROM paymentrecord WHERE recipient = ?";
            try (PreparedStatement stmt = connection.prepareStatement(query)) {
                stmt.setString(1, recordName);
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next(), "Query should return results for " + recordName);
                    int count = rs.getInt(1);
                    assertTrue(
                            count > 0,
                            "Record for " + recordName + " should exist in the database");
                }
            }
        }

        LOG.info("All expected records found in database");
    }

    private static void resetReplayCaptureArtifacts() throws IOException {
        if (!TELEMETRY_CAPTURE_ACTIVE) {
            return;
        }
        clearReplayCaptureDirectory();
        Files.deleteIfExists(REPLAY_FILE);
    }

    private static void clearReplayCaptureDirectory() throws IOException {
        Files.createDirectories(REPLAY_CAPTURE_DIR);
        try (var stream = Files.list(REPLAY_CAPTURE_DIR)) {
            for (Path file : stream.toList()) {
                Files.deleteIfExists(file);
            }
        }
    }

    private static PipelineReplayDocument mergeReplayDocuments(Path replayDir, Path outputFile) throws Exception {
        List<Path> replayFiles;
        try (var files = Files.list(replayDir)) {
            replayFiles = files
                    .filter(path -> path.getFileName().toString().endsWith(".json"))
                    .sorted()
                    .toList();
        }
        assertFalse(replayFiles.isEmpty(), "Expected replay fragments in " + replayDir);

        List<PipelineReplayDocument> documents = new ArrayList<>();
        for (Path replayFile : replayFiles) {
            documents.add(PipelineJson.mapper().readValue(replayFile.toFile(), PipelineReplayDocument.class));
        }
        documents.sort(Comparator.comparing(PipelineReplayDocument::startedAt));

        PipelineReplayDocument canonical = selectCanonicalReplayDocument(documents);
        PipelineReplayRunParameters runParameters = selectMergedRunParameters(documents);
        Instant earliestStart = documents.getFirst().startedAt();
        List<PipelineExecutionEvent> shiftedEvents = new ArrayList<>();
        boolean completedFragmentPresent = documents.stream().anyMatch(document -> "completed".equals(document.status()));
        String status = completedFragmentPresent ? "completed" : canonical.status();
        String failureType = completedFragmentPresent ? null : canonical.failureType();
        String failureMessage = completedFragmentPresent ? null : canonical.failureMessage();
        double maxEndSeconds = 0;

        for (PipelineReplayDocument document : documents) {
            if (!"completed".equals(document.status())
                    && !(completedFragmentPresent && isAwaitSuspensionFragment(document))
                    && "completed".equals(status)) {
                status = document.status();
                failureType = document.failureType();
                failureMessage = document.failureMessage();
            }
            double offsetSeconds = Duration.between(earliestStart, document.startedAt()).toMillis() / 1000.0;
            for (PipelineExecutionEvent event : document.events()) {
                double shiftedStart = offsetSeconds + event.startTime();
                double shiftedEnd = offsetSeconds + event.endTime();
                shiftedEvents.add(new PipelineExecutionEvent(
                        event.traceId(),
                        event.spanId(),
                        event.parentSpanId(),
                        event.itemId(),
                        event.pipeline(),
                        event.step(),
                        event.service(),
                        event.event(),
                        shiftedStart,
                        shiftedEnd,
                        event.durationMs(),
                        event.from(),
                        event.to(),
                        event.cardinality(),
                        event.parentItemIds(),
                        event.sequence(),
                        event.attempt(),
                        event.errorType(),
                        event.errorMessage(),
                        event.attributes()));
                maxEndSeconds = Math.max(maxEndSeconds, shiftedEnd);
            }
        }

        shiftedEvents.sort((left, right) -> {
            double leftTime = playbackTimeForEvent(left);
            double rightTime = playbackTimeForEvent(right);
            if (leftTime != rightTime) {
                return Double.compare(leftTime, rightTime);
            }
            long leftSequence = left.sequence() == null ? 0 : left.sequence();
            long rightSequence = right.sequence() == null ? 0 : right.sequence();
            int sequenceComparison = Long.compare(leftSequence, rightSequence);
            if (sequenceComparison != 0) {
                return sequenceComparison;
            }
            int itemComparison = nullSafe(left.itemId()).compareTo(nullSafe(right.itemId()));
            if (itemComparison != 0) {
                return itemComparison;
            }
            int eventComparison = nullSafe(left.event()).compareTo(nullSafe(right.event()));
            if (eventComparison != 0) {
                return eventComparison;
            }
            int traceComparison = nullSafe(left.traceId()).compareTo(nullSafe(right.traceId()));
            if (traceComparison != 0) {
                return traceComparison;
            }
            int spanComparison = nullSafe(left.spanId()).compareTo(nullSafe(right.spanId()));
            if (spanComparison != 0) {
                return spanComparison;
            }
            int attemptComparison = Integer.compare(
                    left.attempt() == null ? 0 : left.attempt(),
                    right.attempt() == null ? 0 : right.attempt());
            if (attemptComparison != 0) {
                return attemptComparison;
            }
            return nullSafe(left.step()).compareTo(nullSafe(right.step()));
        });

        List<PipelineExecutionEvent> renumbered = new ArrayList<>(shiftedEvents.size());
        for (int index = 0; index < shiftedEvents.size(); index += 1) {
            PipelineExecutionEvent event = shiftedEvents.get(index);
            renumbered.add(new PipelineExecutionEvent(
                    event.traceId(),
                    event.spanId(),
                    event.parentSpanId(),
                    event.itemId(),
                    event.pipeline(),
                    event.step(),
                    event.service(),
                    event.event(),
                    event.startTime(),
                    event.endTime(),
                    event.durationMs(),
                    event.from(),
                    event.to(),
                    event.cardinality(),
                    event.parentItemIds(),
                    (long) index + 1,
                    event.attempt(),
                    event.errorType(),
                    event.errorMessage(),
                    event.attributes()));
        }

        PipelineReplayDocument merged = new PipelineReplayDocument(
                canonical.pipeline(),
                earliestStart,
                Math.round(maxEndSeconds * 1000),
                status,
                failureType,
                failureMessage,
                runParameters,
                canonical.topology(),
                List.copyOf(renumbered));
        Files.createDirectories(outputFile.getParent());
        Files.writeString(
                outputFile,
                PipelineJson.mapper().writerWithDefaultPrettyPrinter().writeValueAsString(merged));
        return merged;
    }

    private static PipelineReplayDocument selectCanonicalReplayDocument(List<PipelineReplayDocument> documents) {
        return documents.stream()
                .max(Comparator.<PipelineReplayDocument>comparingInt(document ->
                                "completed".equals(document.status()) ? 1 : 0)
                        .thenComparingInt(document ->
                                isAwaitSuspensionFragment(document) ? 0 : 1)
                        .thenComparingInt(document ->
                                document.topology() == null || document.topology().steps() == null
                                        ? 0
                                        : document.topology().steps().size())
                        .thenComparingInt(document ->
                                document.topology() == null || document.topology().transitions() == null
                                        ? 0
                                        : document.topology().transitions().size())
                        .thenComparing(PipelineReplayDocument::startedAt))
                .orElseThrow(() -> new IllegalStateException("No replay documents available to merge."));
    }

    private static PipelineReplayRunParameters selectMergedRunParameters(List<PipelineReplayDocument> documents) throws Exception {
        Map<String, PipelineReplayRunParameters> distinctRunParameters = new LinkedHashMap<>();
        for (PipelineReplayDocument document : documents) {
            if (document.runParameters() == null) {
                continue;
            }
            distinctRunParameters.put(
                    PipelineJson.mapper().writeValueAsString(document.runParameters()),
                    document.runParameters());
        }
        if (distinctRunParameters.size() > 1) {
            fail("Replay fragments disagree on runParameters; merged replay parameters must be run-level metadata.");
        }
        return distinctRunParameters.values().stream().findFirst().orElse(null);
    }

    private void assertReplayCoverage(PipelineReplayDocument replayDocument) {
        assertEquals("completed", replayDocument.status(), "Expected merged replay to complete successfully.");
        assertReplayStepEvents(replayDocument, "ProcessCsvPaymentsInput");
        assertReplayStepEvents(replayDocument, "AwaitPaymentProvider");
        assertReplayStepEvents(replayDocument, "ProcessApprovedPaymentStatus");
        assertReplayStepEvents(replayDocument, "ProcessUnapprovedPaymentStatus");
        assertReplayMergeNode(replayDocument, "FinalizePaymentOutput");
        assertReplayStepEvents(replayDocument, "ProcessFinalizePaymentOutput");
        assertReplayStepEvents(replayDocument, "PersistencePaymentRecordSideEffect");
        assertReplayStepEvents(replayDocument, "PersistencePaymentOutputSideEffect");
        assertReplayStepEvents(replayDocument, "ObjectIngest");
        assertReplayStepEvents(replayDocument, "ObjectPublish");
        assertReplayEvent(replayDocument, "object_ingest_submitted");
        assertReplayEvent(replayDocument, "object_publish_published");
        assertNoReplayStepEvents(replayDocument, "ProcessFolder");
        assertNoReplayStepEvents(replayDocument, "ProcessCsvPaymentsOutputFile");
        assertNoReplayStepEvents(replayDocument, "PersistenceCsvPaymentsOutputFileSideEffect");
        assertTrue(
                replayDocument.events().stream().anyMatch(event ->
                        "ProcessCsvPaymentsInput".equals(event.step())
                                && "emit".equals(event.event())
                                && "AwaitPaymentProvider".equals(event.to())),
                "Expected merged replay to contain input-to-await flow events.");
        assertTrue(
                replayDocument.events().stream().anyMatch(event ->
                        ("ProcessApprovedPaymentStatus".equals(event.step())
                                || "ProcessUnapprovedPaymentStatus".equals(event.step()))
                                && "AwaitPaymentProvider".equals(event.from())),
                "Expected merged replay to contain await-resume flow events.");

        PipelineReplayTopology topology = replayDocument.topology();
        assertTrue(topology.transitions().stream().anyMatch(transition ->
                        "await-request".equals(transition.relationKind())),
                "Expected await-request transitions in merged replay topology.");
        assertTrue(topology.transitions().stream().anyMatch(transition ->
                        "await-completion".equals(transition.relationKind())),
                "Expected await-completion transitions in merged replay topology.");
        assertTrue(topology.transitions().stream().anyMatch(transition ->
                        "store".equals(transition.relationKind())),
                "Expected store transitions in merged replay topology.");
        assertReplayLifecycleEvents(replayDocument);
        assertItemizedAwaitLiveFlowStartsBeforeUnitCompletes(replayDocument);
    }

    private void assertReplayLifecycleEvents(PipelineReplayDocument replayDocument) {
        assertReplayEvent(replayDocument, AWAIT_INTERACTION_DISPATCHED);
        assertReplayEvent(replayDocument, AWAIT_UNIT_DISPATCH_COMPLETE);
        assertReplayEvent(replayDocument, AWAIT_UNIT_ITEM_COMPLETED);
        boolean executionWaited = replayDocument.events().stream()
                .anyMatch(event -> AWAIT_EXECUTION_WAITING.equals(event.event()));
        if (executionWaited) {
            assertReplayEvent(replayDocument, AWAIT_RESUME_RELEASED);
        }
        assertTrue(
                replayDocument.events().stream()
                        .filter(event -> AWAIT_UNIT_COMPLETED.equals(event.event())
                                || AWAIT_UNIT_DISPATCH_COMPLETE.equals(event.event()))
                        .map(PipelineExecutionEvent::attributes)
                        .anyMatch(eventAttributes -> eventAttributes != null
                                && "COMPLETED".equals(eventAttributes.get("tpf.await.status"))),
                "Expected await lifecycle events to show unit completion.");
        PipelineExecutionEvent itemCompleted = replayDocument.events().stream()
                .filter(event -> AWAIT_UNIT_ITEM_COMPLETED.equals(event.event()))
                .findFirst()
                .orElseThrow();
        Map<String, String> attributes = itemCompleted.attributes();
        assertTrue(attributes != null && attributes.containsKey("tpf.await.unit_id"),
                "Expected await item completion event to include unit id.");
        assertTrue(attributes.containsKey("tpf.await.interaction_id"),
                "Expected await item completion event to include interaction id.");
        assertTrue(attributes.containsKey("tpf.await.completed_item_count"),
                "Expected await item completion event to include completed item count.");
        assertTrue(
                replayDocument.events().stream()
                        .filter(event -> AWAIT_UNIT_ITEM_COMPLETED.equals(event.event())
                                || AWAIT_UNIT_DISPATCH_COMPLETE.equals(event.event())
                                || AWAIT_UNIT_COMPLETED.equals(event.event()))
                        .map(PipelineExecutionEvent::attributes)
                        .anyMatch(eventAttributes -> eventAttributes != null
                                && eventAttributes.containsKey("tpf.await.expected_item_count")),
                "Expected await lifecycle events to include expected item count once dispatch size is known.");
    }

    private void assertItemizedAwaitLiveFlowStartsBeforeUnitCompletes(PipelineReplayDocument replayDocument) {
        Comparator<PipelineExecutionEvent> playbackOrder = Comparator
                .comparingDouble(AbstractCsvPaymentsEndToEnd::playbackTimeForEvent)
                .thenComparingLong(event -> event.sequence() == null ? Long.MAX_VALUE : event.sequence());
        PipelineExecutionEvent firstPaymentStatusEvent = replayDocument.events().stream()
                .filter(event ->
                        "ProcessApprovedPaymentStatus".equals(event.step())
                                || "ProcessUnapprovedPaymentStatus".equals(event.step()))
                .min(playbackOrder)
                .orElseThrow(() -> new AssertionError("Expected payment-status branch replay events."));
        PipelineExecutionEvent awaitUnitCompletedEvent = replayDocument.events().stream()
                .filter(event -> AWAIT_UNIT_COMPLETED.equals(event.event()))
                .min(playbackOrder)
                .orElseThrow(() -> new AssertionError("Expected await_unit_completed replay events."));
        assertTrue(
                playbackOrder.compare(firstPaymentStatusEvent, awaitUnitCompletedEvent) < 0,
                "Expected connector-first Kafka ONE_TO_ONE await to process completed items through the live session before the await unit completes.");
    }

    private void assertReplayEvent(PipelineReplayDocument replayDocument, String eventName) {
        assertTrue(
                replayDocument.events().stream().anyMatch(event -> eventName.equals(event.event())),
                "Expected merged replay to contain " + eventName + " events.");
    }

    private void assertNoReplayStepEvents(PipelineReplayDocument replayDocument, String stepName) {
        assertFalse(
                replayDocument.events().stream().anyMatch(event ->
                        stepName.equals(event.step())
                                || stepName.equals(event.from())
                                || stepName.equals(event.to())),
                "Expected connector-first replay to omit legacy " + stepName + " events.");
    }

    private static boolean isAwaitSuspensionFragment(PipelineReplayDocument document) {
        return document != null
                && "failed".equals(document.status())
                && "org.pipelineframework.awaitable.AwaitSuspendedException".equals(document.failureType());
    }

    private static double playbackTimeForEvent(PipelineExecutionEvent event) {
        if (event == null) {
            return 0d;
        }
        return "success".equals(event.event()) || "error".equals(event.event())
                ? event.endTime()
                : event.startTime();
    }

    private void assertReplayStepEvents(PipelineReplayDocument replayDocument, String stepName) {
        assertTrue(
                replayDocument.events().stream().anyMatch(event -> stepName.equals(event.step())),
                "Expected merged replay to contain direct events for " + stepName + ".");
    }

    private void assertReplayMergeNode(PipelineReplayDocument replayDocument, String stepName) {
        assertTrue(
                replayDocument.events().stream().anyMatch(event ->
                        stepName.equals(event.from()) || stepName.equals(event.to())),
                "Expected merged replay to contain merge flow events for " + stepName + ".");
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }

    /**
     * Cleans up resources by stopping all containers and closing the network. This method runs
     * after all tests in the class have completed to ensure all Testcontainers resources are
     * properly released.
     */
    @AfterAll
    static void tearDown() {
        if (TELEMETRY_CAPTURE_ACTIVE) {
            if (Files.exists(REPLAY_FILE)) {
                LOG.infof("Replay JSON available at %s.", REPLAY_FILE);
            } else {
                LOG.warnf("Replay JSON was not produced at %s.", REPLAY_FILE);
            }
        }
        if (TEMPO_VERIFICATION_ACTIVE && TEMPO_PAUSE_BEFORE_TEARDOWN) {
            pauseBeforeTempoTeardown();
        }
        // Stop all containers to prevent resource leaks
        if (outputCsvService != null && outputCsvService.isRunning()) {
            outputCsvService.stop();
        }
        if (pipelineRuntimeService != null && pipelineRuntimeService.isRunning()) {
            pipelineRuntimeService.stop();
        }
        if (paymentStatusService != null && paymentStatusService.isRunning()) {
            paymentStatusService.stop();
        }
        if (paymentsProcessingService != null && paymentsProcessingService.isRunning()) {
            paymentsProcessingService.stop();
        }
        if (inputCsvService != null && inputCsvService.isRunning()) {
            inputCsvService.stop();
        }
        if (persistenceService != null && persistenceService.isRunning()) {
            persistenceService.stop();
        }
        if (postgresContainer != null && postgresContainer.isRunning()) {
            postgresContainer.stop();
        }
        if (lgtmStack != null && lgtmStack.isRunning()) {
            lgtmStack.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    private static void pauseBeforeTempoTeardown() {
        LOG.infof("Tempo pause requested. Grafana UI: %s", grafanaBaseUrl());
        LOG.infof("Tempo API: %s", tempoApiBaseUrl());
        LOG.infof(
                "Example trace search: curl -G '%s/api/search' --data-urlencode 'q={ .service.name = \"orchestrator-svc\" }'",
                tempoApiBaseUrl());
        if (System.console() != null) {
            System.console().printf("Tempo pause active. Press ENTER to tear down containers.%n");
            System.console().readLine();
            return;
        }

        long remainingMillis = TimeUnit.SECONDS.toMillis(TEMPO_LOCAL_PAUSE_SECONDS);
        while (remainingMillis > 0L) {
            LOG.infof(
                    "Tempo pause active without interactive console; keeping stack alive for %ds more.",
                    TimeUnit.MILLISECONDS.toSeconds(remainingMillis));
            try {
                long sleepMillis = Math.min(30_000L, remainingMillis);
                Thread.sleep(sleepMillis);
                remainingMillis -= sleepMillis;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Tempo pause interrupted; proceeding with teardown.");
                return;
            }
        }
    }
}
