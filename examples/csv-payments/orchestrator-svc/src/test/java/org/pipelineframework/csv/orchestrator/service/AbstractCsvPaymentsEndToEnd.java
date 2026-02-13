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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class AbstractCsvPaymentsEndToEnd {

    private static final Logger LOG = Logger.getLogger(AbstractCsvPaymentsEndToEnd.class);

    private static final Network network = Network.newNetwork();
    private static final String TEST_E2E_DIR = System.getProperty("user.dir") + "/target/test-e2e";
    private static final String TEST_E2E_TARGET_DIR = "/app/test-e2e";
    private static final Path DEV_CERTS_DIR =
            Paths.get(System.getProperty("user.dir"))
                    .resolve("../target/dev-certs")
                    .normalize()
                    .toAbsolutePath();
    private static final String CONTAINER_KEYSTORE_PATH = "/deployments/server-keystore.jks";
    private static final String CONTAINER_TRUSTSTORE_PATH = "/deployments/client-truststore.jks";
    private static final String RUNTIME_LAYOUT =
            System.getProperty("csv.runtime.layout", "modular").trim().toLowerCase();
    private static final boolean MONOLITH_LAYOUT = "monolith".equals(RUNTIME_LAYOUT);
    private static final boolean PIPELINE_RUNTIME_LAYOUT = "pipeline-runtime".equals(RUNTIME_LAYOUT);
    private static final long PIPELINE_WAIT_TIMEOUT_SECONDS =
            Long.getLong("csv.e2e.pipeline.wait.seconds", 120L);
    private static final long PIPELINE_WAIT_POLL_MILLIS = 1000L;
    // CI sets IMAGE_TAG to github.sha; local fallback should match dev image naming conventions.
    private static final String PIPELINE_RUNTIME_IMAGE =
            System.getenv().getOrDefault("IMAGE_REGISTRY", "registry.example.com")
                    + "/"
                    + System.getenv().getOrDefault("IMAGE_GROUP", "csv-payments")
                    + "/"
                    + System.getenv().getOrDefault("IMAGE_NAME", "pipeline-runtime-svc")
                    + ":"
                    + System.getenv().getOrDefault("IMAGE_TAG", "latest");

    // Containers are lazily created so monolith mode does not require service cert binds.
    static PostgreSQLContainer<?> postgresContainer;
    static GenericContainer<?> persistenceService;
    static GenericContainer<?> inputCsvService;
    static GenericContainer<?> paymentsProcessingService;
    static GenericContainer<?> paymentStatusService;
    static GenericContainer<?> outputCsvService;
    static GenericContainer<?> pipelineRuntimeService;

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
        // Create the test directory if it doesn't exist
        Path dir = Paths.get(TEST_E2E_DIR);
        Files.createDirectories(dir);
        ensureWritable(dir);
        ensureDevCerts();

        if (MONOLITH_LAYOUT) {
            Startables.deepStart(Stream.of(getPostgresContainer())).join();
            return;
        }

        if (PIPELINE_RUNTIME_LAYOUT) {
            Startables.deepStart(
                    Stream.of(getPostgresContainer(), getPersistenceService(), getPipelineRuntimeService()))
                .join();
            return;
        }

        Startables.deepStart(
                        Stream.of(
                                getPostgresContainer(),
                                getPersistenceService(),
                                getInputCsvService(),
                                getPaymentsProcessingService(),
                                getPaymentStatusService(),
                                getOutputCsvService()))
                .join();
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
                    new GenericContainer<>("localhost/csv-payments/persistence-svc:latest")
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
                            .waitingFor(
                                    Wait.forHttps("/q/health")
                                            .forPort(8448)
                                            .allowInsecure()
                                            .withStartupTimeout(Duration.ofSeconds(60)));
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
                    new GenericContainer<>("localhost/csv-payments/input-csv-file-processing-svc:latest")
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
                            .waitingFor(
                                    Wait.forHttps("/q/health")
                                            .forPort(8444)
                                            .allowInsecure()
                                            .withStartupTimeout(Duration.ofSeconds(60)));
        }
        return inputCsvService;
    }

    /**
     * Lazily creates and returns a Testcontainers GenericContainer configured for the payments-processing service.
     *
     * The container is attached to the shared test network, exposes port 8445, mounts the service keystore
     * into the container, sets the Quarkus profile to "test", and waits for an HTTPS health endpoint at /q/health.
     *
     * @return the initialized or previously created GenericContainer for the payments-processing service
     */
    private static GenericContainer<?> getPaymentsProcessingService() {
        if (paymentsProcessingService == null) {
            paymentsProcessingService =
                    new GenericContainer<>("localhost/csv-payments/payments-processing-svc:latest")
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
                            .waitingFor(
                                    Wait.forHttps("/q/health")
                                            .forPort(8445)
                                            .allowInsecure()
                                            .withStartupTimeout(Duration.ofSeconds(60)));
        }
        return paymentsProcessingService;
    }

    /**
     * Create and configure the payment status service Testcontainers GenericContainer for the test network.
     *
     * The container is lazily initialized and configured with network settings, a mounted server keystore,
     * exposed HTTPS port 8446, the `test` Quarkus profile, and a health check against `/q/health`.
     *
     * @return the configured GenericContainer instance for the payment status service
     */
    private static GenericContainer<?> getPaymentStatusService() {
        if (paymentStatusService == null) {
            paymentStatusService =
                    new GenericContainer<>("localhost/csv-payments/payment-status-svc:latest")
                            .withNetwork(network)
                            .withNetworkAliases("payment-status-svc")
                            .withFileSystemBind(
                                    DEV_CERTS_DIR.resolve("payment-status-svc/server-keystore.jks").toString(),
                                    CONTAINER_KEYSTORE_PATH,
                                    BindMode.READ_ONLY)
                            .withExposedPorts(8446)
                            .withEnv("QUARKUS_PROFILE", "test")
                            .withEnv("SERVER_KEYSTORE_PATH", CONTAINER_KEYSTORE_PATH)
                            .waitingFor(
                                    Wait.forHttps("/q/health")
                                            .forPort(8446)
                                            .allowInsecure()
                                            .withStartupTimeout(Duration.ofSeconds(60)));
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
                    new GenericContainer<>("localhost/csv-payments/output-csv-file-processing-svc:latest")
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
                                    Wait.forHttps("/q/health")
                                            .forPort(8447)
                                            .allowInsecure()
                                            .withStartupTimeout(Duration.ofSeconds(60)));
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
                            .withLogConsumer(containerLog("pipeline-runtime-svc"))
                            .waitingFor(
                                    Wait.forHttps("/q/health")
                                            .forPort(8445)
                                            .allowInsecure()
                                            .withStartupTimeout(Duration.ofSeconds(60)));
        }
        return pipelineRuntimeService;
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
            String permission = Files.isDirectory(path) ? "rwxr-xr-x" : "rw-r--r--";
            Set<PosixFilePermission> perms = PosixFilePermissions.fromString(permission);
            Files.setPosixFilePermissions(path, perms);
        } catch (IOException | UnsupportedOperationException e) {
            LOG.warnf("Unable to set permissions on %s: %s", path, e.getMessage());
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
        LOG.info("Running full end-to-end pipeline test");

        // Create test input directory - already created in @BeforeAll
        Path dir = Paths.get(TEST_E2E_DIR);
        // Make sure the directory exists at the time of test execution too
        Files.createDirectories(dir);

        // Clean up any existing test files
        cleanTestOutputDirectory(dir);

        // Create test CSV files as the shell script does
        createTestCsvFiles();

        // Trigger the orchestrator to process the input directory
        String inputPath = MONOLITH_LAYOUT ? TEST_E2E_DIR : TEST_E2E_TARGET_DIR;
        String inputDirJson = "{ \"path\": \"" + inputPath + "\" }";
        orchestratorTriggerRun(inputDirJson);

        // Wait for the pipeline to complete
        waitForPipelineComplete();

        // Verify the output files are generated with expected content
        verifyOutputFiles(TEST_E2E_DIR);

        // Verify database persistence
        verifyDatabasePersistence();

        LOG.info("End-to-end processing test completed successfully!");
    }

    /**
     * Start the orchestrator JAR in a separate JVM configured to use the test services and process CSV files from the given input directory.
     *
     * @param inputDir path to the directory containing input CSV files to process
     * @throws Exception if the process cannot be started, times out, or exits with a non-zero exit code
     */
    @SuppressWarnings("SameParameterValue")
    private void orchestratorTriggerRun(String inputDir) throws Exception {
        LOG.infof("Triggering Orchestrator with input dir: %s", inputDir);

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
                        "-i=" + inputDir);

        pb.environment().put("QUARKUS_PROFILE", "test");
        pb.environment().put("QUARKUS_JIB_JVM_ADDITIONAL_ARGUMENTS", "--enable-preview");

        if (MONOLITH_LAYOUT) {
            configureMonolithEnv(pb);
        } else if (PIPELINE_RUNTIME_LAYOUT) {
            configurePipelineRuntimeEnv(pb);
        } else {
            configureModularEnv(pb);
        }

        pb.inheritIO();
        Process p = pb.start();
        boolean completed = p.waitFor(120, TimeUnit.SECONDS);
        assertTrue(completed, "Orchestrator process timed out");
        int exitCode = p.exitValue();
        assertEquals(0, exitCode, "Orchestrator exited with non-zero code");
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
        putGrpcClient(pb, "PROCESS_FOLDER", localhost, grpcPort);
        putGrpcClient(pb, "PROCESS_CSV_PAYMENTS_INPUT", localhost, grpcPort);
        putGrpcClient(pb, "PROCESS_SEND_PAYMENT_RECORD", localhost, grpcPort);
        putGrpcClient(pb, "PROCESS_ACK_PAYMENT_SENT", localhost, grpcPort);
        putGrpcClient(pb, "PROCESS_PAYMENT_STATUS", localhost, grpcPort);
        putGrpcClient(pb, "PROCESS_CSV_PAYMENTS_OUTPUT_FILE", localhost, grpcPort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_CSV_PAYMENTS_INPUT_FILE_SIDE_EFFECT", localhost, grpcPort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_PAYMENT_RECORD_SIDE_EFFECT", localhost, grpcPort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_ACK_PAYMENT_SENT_SIDE_EFFECT", localhost, grpcPort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_PAYMENT_STATUS_SIDE_EFFECT", localhost, grpcPort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_CSV_PAYMENTS_OUTPUT_FILE_SIDE_EFFECT", localhost, grpcPort);
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
        GenericContainer<?> outputService = getOutputCsvService();
        GenericContainer<?> persistence = getPersistenceService();
        pb.environment()
            .put(
                "SERVER_KEYSTORE_PATH",
                DEV_CERTS_DIR.resolve("orchestrator-svc/server-keystore.jks").toString());
        pb.environment()
            .put(
                "CLIENT_TRUSTSTORE_PATH",
                DEV_CERTS_DIR.resolve("orchestrator-svc/client-truststore.jks").toString());
        putGrpcClient(pb, "PROCESS_FOLDER", inputService.getHost(), String.valueOf(inputService.getMappedPort(8444)));
        putGrpcClient(pb, "PROCESS_CSV_PAYMENTS_INPUT", inputService.getHost(), String.valueOf(inputService.getMappedPort(8444)));
        putGrpcClient(pb, "PROCESS_SEND_PAYMENT_RECORD", paymentsService.getHost(), String.valueOf(paymentsService.getMappedPort(8445)));
        putGrpcClient(pb, "PROCESS_ACK_PAYMENT_SENT", paymentsService.getHost(), String.valueOf(paymentsService.getMappedPort(8445)));
        putGrpcClient(pb, "PROCESS_PAYMENT_STATUS", statusService.getHost(), String.valueOf(statusService.getMappedPort(8446)));
        putGrpcClient(pb, "PROCESS_CSV_PAYMENTS_OUTPUT_FILE", outputService.getHost(), String.valueOf(outputService.getMappedPort(8447)));
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_CSV_PAYMENTS_INPUT_FILE_SIDE_EFFECT", persistence.getHost(), String.valueOf(persistence.getMappedPort(8448)));
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_PAYMENT_RECORD_SIDE_EFFECT", persistence.getHost(), String.valueOf(persistence.getMappedPort(8448)));
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_ACK_PAYMENT_SENT_SIDE_EFFECT", persistence.getHost(), String.valueOf(persistence.getMappedPort(8448)));
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_PAYMENT_STATUS_SIDE_EFFECT", persistence.getHost(), String.valueOf(persistence.getMappedPort(8448)));
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_CSV_PAYMENTS_OUTPUT_FILE_SIDE_EFFECT", persistence.getHost(), String.valueOf(persistence.getMappedPort(8448)));
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_PAYMENT_OUTPUT_SIDE_EFFECT", persistence.getHost(), String.valueOf(persistence.getMappedPort(8448)));
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

        putGrpcClient(pb, "PROCESS_FOLDER", pipelineHost, pipelinePort);
        putGrpcClient(pb, "PROCESS_CSV_PAYMENTS_INPUT", pipelineHost, pipelinePort);
        putGrpcClient(pb, "PROCESS_SEND_PAYMENT_RECORD", pipelineHost, pipelinePort);
        putGrpcClient(pb, "PROCESS_ACK_PAYMENT_SENT", pipelineHost, pipelinePort);
        putGrpcClient(pb, "PROCESS_PAYMENT_STATUS", pipelineHost, pipelinePort);
        putGrpcClient(pb, "PROCESS_CSV_PAYMENTS_OUTPUT_FILE", pipelineHost, pipelinePort);

        putGrpcClient(pb, "OBSERVE_PERSISTENCE_CSV_PAYMENTS_INPUT_FILE_SIDE_EFFECT", persistenceHost, persistencePort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_PAYMENT_RECORD_SIDE_EFFECT", persistenceHost, persistencePort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_ACK_PAYMENT_SENT_SIDE_EFFECT", persistenceHost, persistencePort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_PAYMENT_STATUS_SIDE_EFFECT", persistenceHost, persistencePort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_CSV_PAYMENTS_OUTPUT_FILE_SIDE_EFFECT", persistenceHost, persistencePort);
        putGrpcClient(pb, "OBSERVE_PERSISTENCE_PAYMENT_OUTPUT_SIDE_EFFECT", persistenceHost, persistencePort);
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

            if (outputFilesExist) {
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

        fail(
                "Pipeline completion timeout reached with no .out files in "
                        + TEST_E2E_DIR
                        + " after "
                        + PIPELINE_WAIT_TIMEOUT_SECONDS
                        + "s");
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

        // Count total records in all output files (excluding headers)
        long totalRecords =
                outputFiles.stream()
                        .mapToLong(
                                path -> {
                                    try {
                                        List<String> lines = Files.readAllLines(path);
                                        // Exclude header line (first line)
                                        return Math.max(0, lines.size() - 1);
                                    } catch (IOException e) {
                                        LOG.warnf(e, "Failed to read file: %s", path);
                                        return 0L;
                                    }
                                })
                        .sum();

        LOG.infof("Total records across all output files: %d", totalRecords);

        // Expected: at least 5 records total (3 from first file + 2 from second file)
        assertTrue(
                totalRecords >= 5,
                String.format("Expected at least 5 records, but found %d", totalRecords));

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

                // Expected: 5 records total (3 from first file + 2 from second file)
                assertEquals(
                        5,
                        recordCount,
                        String.format("Expected 5 records in database, but found %d", recordCount));
            }

            // Verify specific records exist in the database
            verifySpecificRecordsInDatabase(connection);

            LOG.info("Database verification completed successfully");
        }
    }

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

    /**
     * Cleans up resources by stopping all containers and closing the network. This method runs
     * after all tests in the class have completed to ensure all Testcontainers resources are
     * properly released.
     */
    @AfterAll
    static void tearDown() {
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
        if (network != null) {
            network.close();
        }
    }
}
