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
    private static final String RUNTIME_LAYOUT =
            System.getProperty("csv.runtime.layout", "modular").trim().toLowerCase();
    private static final boolean MONOLITH_LAYOUT = "monolith".equals(RUNTIME_LAYOUT);

    // Containers are lazily created so monolith mode does not require service cert binds.
    static PostgreSQLContainer<?> postgresContainer;
    static GenericContainer<?> persistenceService;
    static GenericContainer<?> inputCsvService;
    static GenericContainer<?> paymentsProcessingService;
    static GenericContainer<?> paymentStatusService;
    static GenericContainer<?> outputCsvService;

    /**
     * Initialises and starts the test containers required for the end-to-end CSV payments test.
     *
     * <p>Creates the test directory first if needed, then starts the PostgreSQL container, then
     * starts each service container so the test services (input CSV, payments processing, payment
     * status and output CSV) are available before tests execute.
     */
    @BeforeAll
    static void startServices() throws IOException {
        // Create the test directory if it doesn't exist
        Path dir = Paths.get(TEST_E2E_DIR);
        Files.createDirectories(dir);
        ensureWritable(dir);
        ensureDevCerts();

        if (MONOLITH_LAYOUT) {
            Startables.deepStart(java.util.stream.Stream.of(getPostgresContainer())).join();
            return;
        }

        Startables.deepStart(
                        java.util.stream.Stream.of(
                                getPostgresContainer(),
                                getPersistenceService(),
                                getInputCsvService(),
                                getPaymentsProcessingService(),
                                getPaymentStatusService(),
                                getOutputCsvService()))
                .join();
    }

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

    private static void ensureWritable(Path path) {
        try {
            String permission = Files.isDirectory(path) ? "rwxr-xr-x" : "rw-r--r--";
            Set<PosixFilePermission> perms = PosixFilePermissions.fromString(permission);
            Files.setPosixFilePermissions(path, perms);
        } catch (IOException | UnsupportedOperationException e) {
            LOG.warnf("Unable to set permissions on %s: %s", path, e.getMessage());
        }
    }

    private static void ensureDevCerts() throws IOException {
        Path serverKeystore = DEV_CERTS_DIR.resolve("orchestrator-svc/server-keystore.jks");
        Path truststore = DEV_CERTS_DIR.resolve("orchestrator-svc/client-truststore.jks");
        if (Files.exists(serverKeystore) && Files.exists(truststore)) {
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
     * Launches the orchestrator JAR as a separate JVM process configured to use the test services
     * and the given input directory.
     *
     * @param inputDir path to the directory containing input CSV files that the orchestrator should
     *     process
     * @throws Exception if the process cannot be started or if it exits with a non-zero exit code
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
        pb.environment().put("PERSISTENCE_PROVIDER_CLASS", "org.pipelineframework.plugin.persistence.provider.VThreadPersistenceProvider");
        pb.environment().put("QUARKUS_HIBERNATE_ORM_BLOCKING", "true");
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

    private static void putGrpcClient(ProcessBuilder pb, String clientName, String host, String port) {
        pb.environment().put("QUARKUS_GRPC_CLIENTS_" + clientName + "_HOST", host);
        pb.environment().put("QUARKUS_GRPC_CLIENTS_" + clientName + "_PORT", port);
    }

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
     * Create two CSV input files in the test directory containing five payment records used by the
     * end-to-end test.
     *
     * <p>The files created are "payments_first.csv" (three records) and "payments_second.csv" (two
     * records). The method logs the created CSV filenames.
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
     * Waits for pipeline output files to appear in the test directory by polling for a limited
     * time.
     *
     * <p>Polls the TEST_E2E_DIR for files whose names end with ".out", checking once per second and
     * returning as soon as any such file is detected. If no output files are found within the
     * 10-second timeout the method fails the test.
     *
     * @throws InterruptedException if the thread is interrupted while sleeping between polls
     * @throws IOException if an I/O error occurs when listing the test directory
     */
    @SuppressWarnings("BusyWait")
    private void waitForPipelineComplete() throws InterruptedException, IOException {
        LOG.info("Waiting for pipeline to complete processing...");

        // Check for output files to be created before continuing
        long startTime = System.currentTimeMillis();
        long timeout = TimeUnit.SECONDS.toMillis(10); // 10-second timeout

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

            Thread.sleep(1000); // Check every second
        }

        fail("Pipeline completion timeout reached with no .out files in " + TEST_E2E_DIR);
    }

    /**
     * Verifies generated output files in the given target directory contain the expected records.
     *
     * <p>Checks that at least one `.out` file exists, that the combined number of data records
     * (excluding header lines) is at least five, and that output contains records for the
     * recipients John Doe, Jane Smith, Bob Johnson, Alice Brown and Charlie Wilson.
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
     * Verify that expected payment recipient records exist in the database.
     *
     * <p>Checks that a row exists in the `paymentrecord` table for each of the predefined recipient
     * names and fails the test if any expected record is missing.
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
