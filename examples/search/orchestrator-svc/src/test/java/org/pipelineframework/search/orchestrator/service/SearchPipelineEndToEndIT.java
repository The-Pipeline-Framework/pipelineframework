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

package org.pipelineframework.search.orchestrator.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusIntegrationTest
class SearchPipelineEndToEndIT {

    private static final Logger LOG = Logger.getLogger(SearchPipelineEndToEndIT.class);
    private static final Network NETWORK = Network.newNetwork();

    private static final String CRAWL_IMAGE = System.getProperty(
        "search.image.crawl-source", "localhost/search-pipeline/crawl-source-svc:latest");
    private static final String PARSE_IMAGE = System.getProperty(
        "search.image.parse-document", "localhost/search-pipeline/parse-document-svc:latest");
    private static final String TOKENIZE_IMAGE = System.getProperty(
        "search.image.tokenize-content", "localhost/search-pipeline/tokenize-content-svc:latest");
    private static final String INDEX_IMAGE = System.getProperty(
        "search.image.index-document", "localhost/search-pipeline/index-document-svc:latest");
    private static final String PERSISTENCE_IMAGE = System.getProperty(
        "search.image.persistence", "localhost/search/persistence-svc:latest");
    private static final String CACHE_INVALIDATION_IMAGE = System.getProperty(
        "search.image.cache-invalidation", "localhost/search/cache-invalidation-svc:latest");

    private static final PostgreSQLContainer<?> postgres =
        new PostgreSQLContainer<>("postgres:18")
            .withDatabaseName("quarkus")
            .withUsername("quarkus")
            .withPassword("quarkus")
            .withNetwork(NETWORK)
            .withNetworkAliases("postgres")
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60)));

    private static final GenericContainer<?> redis =
        new GenericContainer<>("redis:7-alpine")
            .withNetwork(NETWORK)
            .withNetworkAliases("redis")
            .withExposedPorts(6379)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(30)));

    private static final GenericContainer<?> crawlService =
        new GenericContainer<>(CRAWL_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("crawl-source-svc")
            .withExposedPorts(8444)
            .withEnv("QUARKUS_PROFILE", "test")
            .waitingFor(
                Wait.forHttps("/q/health")
                    .forPort(8444)
                    .allowInsecure()
                    .withStartupTimeout(Duration.ofSeconds(60)));

    private static final GenericContainer<?> parseService =
        new GenericContainer<>(PARSE_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("parse-document-svc")
            .withExposedPorts(8445)
            .withEnv("QUARKUS_PROFILE", "test")
            .waitingFor(
                Wait.forHttps("/q/health")
                    .forPort(8445)
                    .allowInsecure()
                    .withStartupTimeout(Duration.ofSeconds(60)));

    private static final GenericContainer<?> tokenizeService =
        new GenericContainer<>(TOKENIZE_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("tokenize-content-svc")
            .withExposedPorts(8446)
            .withEnv("QUARKUS_PROFILE", "test")
            .waitingFor(
                Wait.forHttps("/q/health")
                    .forPort(8446)
                    .allowInsecure()
                    .withStartupTimeout(Duration.ofSeconds(60)));

    private static final GenericContainer<?> indexService =
        new GenericContainer<>(INDEX_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("index-document-svc")
            .withExposedPorts(8447)
            .withEnv("QUARKUS_PROFILE", "test")
            .waitingFor(
                Wait.forHttps("/q/health")
                    .forPort(8447)
                    .allowInsecure()
                    .withStartupTimeout(Duration.ofSeconds(60)));

    private static final GenericContainer<?> persistenceService =
        new GenericContainer<>(PERSISTENCE_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("persistence-svc")
            .withExposedPorts(8448)
            .withEnv("QUARKUS_PROFILE", "test")
            .withEnv("QUARKUS_DATASOURCE_REACTIVE_URL", "postgresql://postgres:5432/quarkus")
            .withEnv("QUARKUS_DATASOURCE_USERNAME", "quarkus")
            .withEnv("QUARKUS_DATASOURCE_PASSWORD", "quarkus")
            .waitingFor(
                Wait.forHttps("/q/health")
                    .forPort(8448)
                    .allowInsecure()
                    .withStartupTimeout(Duration.ofSeconds(60)));

    private static final GenericContainer<?> cacheInvalidationService =
        new GenericContainer<>(CACHE_INVALIDATION_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("cache-invalidation-svc")
            .withExposedPorts(8449)
            .withEnv("QUARKUS_PROFILE", "test")
            .withEnv("PIPELINE_CACHE_PROVIDER", "redis")
            .withEnv("QUARKUS_REDIS_HOSTS", "redis://redis:6379")
            .waitingFor(
                Wait.forHttps("/q/health")
                    .forPort(8449)
                    .allowInsecure()
                    .withStartupTimeout(Duration.ofSeconds(60)));

    @BeforeAll
    static void startServices() {
        postgres.start();
        redis.start();
        crawlService.start();
        parseService.start();
        tokenizeService.start();
        indexService.start();
        persistenceService.start();
        cacheInvalidationService.start();
    }

    @AfterAll
    static void stopServices() {
        cacheInvalidationService.stop();
        persistenceService.stop();
        indexService.stop();
        tokenizeService.stop();
        parseService.stop();
        crawlService.stop();
        redis.stop();
        postgres.stop();
    }

    @Test
    void requireCacheFailsOnColdCache() throws Exception {
        String version = "cold-" + UUID.randomUUID();
        ProcessResult result = orchestratorTriggerRun("https://example.com", "require-cache", version);
        assertExitFailure(result, "Expected require-cache to fail on a cold cache");
    }

    @Test
    void preferCacheWarmsCacheAndRequireCacheSucceeds() throws Exception {
        String version = "warm-" + UUID.randomUUID();
        ProcessResult warm = orchestratorTriggerRun("https://example.com", "prefer-cache", version);
        assertExitSuccess(warm, "Expected prefer-cache run to succeed");

        ProcessResult require = orchestratorTriggerRun("https://example.com", "require-cache", version);
        assertExitSuccess(require, "Expected require-cache to succeed after warm cache");
    }

    @Test
    void versionTagIsolatesReplay() throws Exception {
        String versionA = "replay-" + UUID.randomUUID();
        String versionB = "rewind-" + UUID.randomUUID();

        ProcessResult warm = orchestratorTriggerRun("https://example.com", "prefer-cache", versionA);
        assertExitSuccess(warm, "Expected prefer-cache run to succeed");

        ProcessResult requireSame = orchestratorTriggerRun("https://example.com", "require-cache", versionA);
        assertExitSuccess(requireSame, "Expected require-cache to succeed for same version");

        ProcessResult requireOther = orchestratorTriggerRun("https://example.com", "require-cache", versionB);
        assertExitFailure(requireOther, "Expected require-cache to fail for a new version tag");
    }

    @Test
    void bypassCacheDoesNotWarmCache() throws Exception {
        String version = "bypass-" + UUID.randomUUID();
        ProcessResult bypass = orchestratorTriggerRun("https://example.com", "bypass-cache", version);
        assertExitSuccess(bypass, "Expected bypass-cache run to succeed");

        ProcessResult require = orchestratorTriggerRun("https://example.com", "require-cache", version);
        assertExitFailure(require, "Expected require-cache to fail after bypass-cache run");
    }

    @Test
    void cacheOnlyWarmsCache() throws Exception {
        String version = "cache-only-" + UUID.randomUUID();
        ProcessResult cacheOnly = orchestratorTriggerRun("https://example.com", "cache-only", version);
        assertExitSuccess(cacheOnly, "Expected cache-only run to succeed");

        ProcessResult require = orchestratorTriggerRun("https://example.com", "require-cache", version);
        assertExitSuccess(require, "Expected require-cache to succeed after cache-only run");
    }

    @Test
    void persistenceWritesOutputs() throws Exception {
        String version = "persist-" + UUID.randomUUID();
        ProcessResult result = orchestratorTriggerRun("https://example.com", "prefer-cache", version);
        assertExitSuccess(result, "Expected pipeline run to succeed");

        assertTrue(countRows("rawdocument") > 0, "Expected RawDocument persistence");
        assertTrue(countRows("parseddocument") > 0, "Expected ParsedDocument persistence");
        assertTrue(countRows("tokenbatch") > 0, "Expected TokenBatch persistence");
        assertTrue(countRows("indexack") > 0, "Expected IndexAck persistence");
    }

    private ProcessResult orchestratorTriggerRun(String input, String cachePolicy, String versionTag) throws Exception {
        ProcessBuilder pb =
            new ProcessBuilder(
                "java",
                "--enable-preview",
                "-jar",
                "target/quarkus-app/quarkus-run.jar",
                "-i=" + input);

        pb.environment().put("QUARKUS_PROFILE", "test");
        pb.environment().put("PIPELINE_CACHE_POLICY", cachePolicy);
        pb.environment().put("PIPELINE_VERSION", versionTag);

        applyRestClientUrl(pb, "PROCESS_CRAWL_SOURCE", crawlService, 8444);
        applyRestClientUrl(pb, "PROCESS_PARSE_DOCUMENT", parseService, 8445);
        applyRestClientUrl(pb, "PROCESS_TOKENIZE_CONTENT", tokenizeService, 8446);
        applyRestClientUrl(pb, "PROCESS_INDEX_DOCUMENT", indexService, 8447);
        applyRestClientUrl(pb, "ORCHESTRATOR_SERVICE", "https://localhost:8443");

        applyRestClientUrl(pb, "OBSERVE_PERSISTENCE_RAW_DOCUMENT_SIDE_EFFECT", persistenceService, 8448);
        applyRestClientUrl(pb, "OBSERVE_PERSISTENCE_PARSED_DOCUMENT_SIDE_EFFECT", persistenceService, 8448);
        applyRestClientUrl(pb, "OBSERVE_PERSISTENCE_TOKEN_BATCH_SIDE_EFFECT", persistenceService, 8448);
        applyRestClientUrl(pb, "OBSERVE_PERSISTENCE_INDEX_ACK_SIDE_EFFECT", persistenceService, 8448);

        applyRestClientUrl(pb, "OBSERVE_CACHE_INVALIDATE_CRAWL_REQUEST_SIDE_EFFECT", cacheInvalidationService, 8449);
        applyRestClientUrl(pb, "OBSERVE_CACHE_INVALIDATE_RAW_DOCUMENT_SIDE_EFFECT", cacheInvalidationService, 8449);
        applyRestClientUrl(pb, "OBSERVE_CACHE_INVALIDATE_PARSED_DOCUMENT_SIDE_EFFECT", cacheInvalidationService, 8449);
        applyRestClientUrl(pb, "OBSERVE_CACHE_INVALIDATE_TOKEN_BATCH_SIDE_EFFECT", cacheInvalidationService, 8449);
        applyRestClientUrl(pb, "OBSERVE_CACHE_INVALIDATE_ALL_CRAWL_REQUEST_SIDE_EFFECT", cacheInvalidationService, 8449);
        applyRestClientUrl(pb, "OBSERVE_CACHE_INVALIDATE_ALL_RAW_DOCUMENT_SIDE_EFFECT", cacheInvalidationService, 8449);
        applyRestClientUrl(pb, "OBSERVE_CACHE_INVALIDATE_ALL_PARSED_DOCUMENT_SIDE_EFFECT", cacheInvalidationService, 8449);
        applyRestClientUrl(pb, "OBSERVE_CACHE_INVALIDATE_ALL_TOKEN_BATCH_SIDE_EFFECT", cacheInvalidationService, 8449);

        pb.redirectErrorStream(true);
        Process process = pb.start();
        String output = readOutput(process);
        boolean finished = process.waitFor(120, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
            throw new IllegalStateException("Orchestrator did not finish in time");
        }
        int exitCode = process.exitValue();
        LOG.infof("Orchestrator output:%n%s", output);
        return new ProcessResult(exitCode, output);
    }

    private void applyRestClientUrl(ProcessBuilder pb, String clientName, GenericContainer<?> container, int port) {
        String url = "https://" + container.getHost() + ":" + container.getMappedPort(port);
        applyRestClientUrl(pb, clientName, url);
    }

    private void applyRestClientUrl(ProcessBuilder pb, String clientName, String url) {
        pb.environment().put("QUARKUS_REST_CLIENT_" + clientName + "_URL", url);
    }

    private String readOutput(Process process) throws IOException {
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line).append(System.lineSeparator());
            }
        }
        return builder.toString();
    }

    private int countRows(String table) throws SQLException {
        String sql = "select count(*) from " + table;
        try (Connection connection = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
             PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }

    private void assertExitSuccess(ProcessResult result, String message) {
        assertTrue(result.exitCode == 0, message + ": " + result.output);
        assertTrue(result.output.contains("Pipeline execution completed"),
            "Expected completion message");
    }

    private void assertExitFailure(ProcessResult result, String message) {
        assertTrue(result.exitCode != 0, message + ": " + result.output);
    }

    private record ProcessResult(int exitCode, String output) {
    }
}
