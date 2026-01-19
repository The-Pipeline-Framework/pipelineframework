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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pipelineframework.search.common.domain.ParsedDocument;
import org.pipelineframework.search.common.util.HashingUtils;
import org.testcontainers.containers.*;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SearchPipelineEndToEndIT {

    private static final Logger LOG = Logger.getLogger(SearchPipelineEndToEndIT.class);
    private static final Network NETWORK = Network.newNetwork();
    private static final String CACHE_PREFIX = "pipeline-cache:";
    private static final Path DEV_CERTS_DIR =
        Paths.get(System.getProperty("user.dir"))
            .resolve("../target/dev-certs")
            .normalize()
            .toAbsolutePath();
    private static final String CONTAINER_KEYSTORE_PATH = "/deployments/server-keystore.jks";
    private static final String CONTAINER_TRUSTSTORE_PATH = "/deployments/client-truststore.jks";

    private static final String CRAWL_IMAGE = System.getProperty(
        "search.image.crawl-source", "localhost/search-pipeline/crawl-source-svc:latest");
    private static final String PARSE_IMAGE = System.getProperty(
        "search.image.parse-document", "localhost/search-pipeline/parse-document-svc:latest");
    private static final String TOKENIZE_IMAGE = System.getProperty(
        "search.image.tokenize-content", "localhost/search-pipeline/tokenize-content-svc:latest");
    private static final String INDEX_IMAGE = System.getProperty(
        "search.image.index-document", "localhost/search-pipeline/index-document-svc:latest");
    private static final String ORCHESTRATOR_IMAGE = System.getProperty(
        "search.image.orchestrator", "localhost/search-pipeline/orchestrator-svc:latest");
    private static final String PERSISTENCE_IMAGE = System.getProperty(
        "search.image.persistence", "localhost/search/persistence-svc:latest");
    private static final String CACHE_INVALIDATION_IMAGE = System.getProperty(
        "search.image.cache-invalidation", "localhost/search/cache-invalidation-svc:latest");

    private static final PostgreSQLContainer<?> postgres =
        new PostgreSQLContainer<>("postgres:17")
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
            .withFileSystemBind(
                DEV_CERTS_DIR.resolve("crawl-source-svc/server-keystore.jks").toString(),
                CONTAINER_KEYSTORE_PATH,
                BindMode.READ_ONLY)
            .withExposedPorts(8080)
            .withEnv("QUARKUS_PROFILE", "test")
            .withEnv("SERVER_KEYSTORE_PATH", CONTAINER_KEYSTORE_PATH)
            .withEnv("QUARKUS_HTTP_INSECURE_REQUESTS", "enabled")
            .withEnv("QUARKUS_HTTP_PORT", "8080")
            .waitingFor(
                Wait.forHttp("/q/health")
                    .forPort(8080)
                    .withStartupTimeout(Duration.ofSeconds(60)));

    private static final GenericContainer<?> parseService =
        new GenericContainer<>(PARSE_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("parse-document-svc")
            .withFileSystemBind(
                DEV_CERTS_DIR.resolve("parse-document-svc/server-keystore.jks").toString(),
                CONTAINER_KEYSTORE_PATH,
                BindMode.READ_ONLY)
            .withExposedPorts(8080)
            .withEnv("QUARKUS_PROFILE", "test")
            .withEnv("SERVER_KEYSTORE_PATH", CONTAINER_KEYSTORE_PATH)
            .withEnv("QUARKUS_HTTP_INSECURE_REQUESTS", "enabled")
            .withEnv("QUARKUS_HTTP_PORT", "8080")
            .waitingFor(
                Wait.forHttp("/q/health")
                    .forPort(8080)
                    .withStartupTimeout(Duration.ofSeconds(60)));

    private static final GenericContainer<?> tokenizeService =
        new GenericContainer<>(TOKENIZE_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("tokenize-content-svc")
            .withFileSystemBind(
                DEV_CERTS_DIR.resolve("tokenize-content-svc/server-keystore.jks").toString(),
                CONTAINER_KEYSTORE_PATH,
                BindMode.READ_ONLY)
            .withExposedPorts(8080)
            .withEnv("QUARKUS_PROFILE", "test")
            .withEnv("SERVER_KEYSTORE_PATH", CONTAINER_KEYSTORE_PATH)
            .withEnv("QUARKUS_HTTP_INSECURE_REQUESTS", "enabled")
            .withEnv("QUARKUS_HTTP_PORT", "8080")
            .waitingFor(
                Wait.forHttp("/q/health")
                    .forPort(8080)
                    .withStartupTimeout(Duration.ofSeconds(60)));

    private static final GenericContainer<?> indexService =
        new GenericContainer<>(INDEX_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("index-document-svc")
            .withFileSystemBind(
                DEV_CERTS_DIR.resolve("index-document-svc/server-keystore.jks").toString(),
                CONTAINER_KEYSTORE_PATH,
                BindMode.READ_ONLY)
            .withExposedPorts(8080)
            .withEnv("QUARKUS_PROFILE", "test")
            .withEnv("SERVER_KEYSTORE_PATH", CONTAINER_KEYSTORE_PATH)
            .withEnv("QUARKUS_HTTP_INSECURE_REQUESTS", "enabled")
            .withEnv("QUARKUS_HTTP_PORT", "8080")
            .waitingFor(
                Wait.forHttp("/q/health")
                    .forPort(8080)
                    .withStartupTimeout(Duration.ofSeconds(60)));

    private static final GenericContainer<?> persistenceService =
        new GenericContainer<>(PERSISTENCE_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("persistence-svc")
            .withFileSystemBind(
                DEV_CERTS_DIR.resolve("persistence-svc/server-keystore.jks").toString(),
                CONTAINER_KEYSTORE_PATH,
                BindMode.READ_ONLY)
            .withExposedPorts(8080)
            .withEnv("QUARKUS_PROFILE", "test")
            .withEnv("SERVER_KEYSTORE_PATH", CONTAINER_KEYSTORE_PATH)
            .withEnv("QUARKUS_HIBERNATE_ORM_SCHEMA_MANAGEMENT_STRATEGY", "drop-and-create")
            .withEnv("QUARKUS_HTTP_INSECURE_REQUESTS", "enabled")
            .withEnv("QUARKUS_HTTP_PORT", "8080")
            .withEnv("QUARKUS_DATASOURCE_REACTIVE_URL", "postgresql://postgres:5432/quarkus")
            .withEnv("QUARKUS_DATASOURCE_USERNAME", "quarkus")
            .withEnv("QUARKUS_DATASOURCE_PASSWORD", "quarkus")
            .waitingFor(
                Wait.forHttp("/q/health")
                    .forPort(8080)
                    .withStartupTimeout(Duration.ofSeconds(60)));

    private static final GenericContainer<?> cacheInvalidationService =
        new GenericContainer<>(CACHE_INVALIDATION_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("cache-invalidation-svc")
            .withFileSystemBind(
                DEV_CERTS_DIR.resolve("cache-invalidation-svc/server-keystore.jks").toString(),
                CONTAINER_KEYSTORE_PATH,
                BindMode.READ_ONLY)
            .withExposedPorts(8080)
            .withEnv("QUARKUS_PROFILE", "test")
            .withEnv("SERVER_KEYSTORE_PATH", CONTAINER_KEYSTORE_PATH)
            .withEnv("QUARKUS_HTTP_INSECURE_REQUESTS", "enabled")
            .withEnv("QUARKUS_HTTP_PORT", "8080")
            .withEnv("PIPELINE_CACHE_PROVIDER", "redis")
            .withEnv("QUARKUS_REDIS_HOSTS", "redis://redis:6379")
            .waitingFor(
                Wait.forHttp("/q/health")
                    .forPort(8080)
                    .withStartupTimeout(Duration.ofSeconds(60)));

    private static final GenericContainer<?> orchestratorService =
        new GenericContainer<>(ORCHESTRATOR_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("orchestrator-svc")
            .withFileSystemBind(
                DEV_CERTS_DIR.resolve("orchestrator-svc/server-keystore.jks").toString(),
                CONTAINER_KEYSTORE_PATH,
                BindMode.READ_ONLY)
            .withFileSystemBind(
                DEV_CERTS_DIR.resolve("orchestrator-svc/client-truststore.jks").toString(),
                CONTAINER_TRUSTSTORE_PATH,
                BindMode.READ_ONLY)
            .withExposedPorts(8080)
            .withEnv("QUARKUS_PROFILE", "test")
            .withEnv("SERVER_KEYSTORE_PATH", CONTAINER_KEYSTORE_PATH)
            .withEnv("CLIENT_TRUSTSTORE_PATH", CONTAINER_TRUSTSTORE_PATH)
            .withEnv("PIPELINE_CACHE_PROVIDER", "redis")
            .withEnv("QUARKUS_REDIS_HOSTS", "redis://redis:6379")
            .withEnv("QUARKUS_HTTP_INSECURE_REQUESTS", "enabled")
            .withEnv("QUARKUS_HTTP_PORT", "8080")
            .withEnv("QUARKUS_REST_CLIENT_PROCESS_CRAWL_SOURCE_URL", "http://crawl-source-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_PROCESS_PARSE_DOCUMENT_URL", "http://parse-document-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_PROCESS_TOKENIZE_CONTENT_URL", "http://tokenize-content-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_PROCESS_INDEX_DOCUMENT_URL", "http://index-document-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_ORCHESTRATOR_SERVICE_URL", "http://orchestrator-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_PERSISTENCE_RAW_DOCUMENT_SIDE_EFFECT_URL", "http://persistence-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_PERSISTENCE_PARSED_DOCUMENT_SIDE_EFFECT_URL", "http://persistence-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_PERSISTENCE_TOKEN_BATCH_SIDE_EFFECT_URL", "http://persistence-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_PERSISTENCE_INDEX_ACK_SIDE_EFFECT_URL", "http://persistence-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_RAW_DOCUMENT_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_PARSED_DOCUMENT_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_TOKEN_BATCH_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_INDEX_ACK_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_INVALIDATE_CRAWL_REQUEST_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_INVALIDATE_RAW_DOCUMENT_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_INVALIDATE_PARSED_DOCUMENT_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_INVALIDATE_TOKEN_BATCH_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_INVALIDATE_ALL_CRAWL_REQUEST_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_INVALIDATE_ALL_RAW_DOCUMENT_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_INVALIDATE_ALL_PARSED_DOCUMENT_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_INVALIDATE_ALL_TOKEN_BATCH_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .waitingFor(
                Wait.forHttp("/q/health")
                    .forPort(8080)
                    .withStartupTimeout(Duration.ofSeconds(60)));

    @BeforeAll
    static void startServices() {
        Startables.deepStart(java.util.stream.Stream.of(
            postgres,
            redis,
            crawlService,
            parseService,
            tokenizeService,
            indexService,
            persistenceService,
            cacheInvalidationService,
            orchestratorService
        )).join();
    }

    @AfterAll
    static void stopServices() {
        orchestratorService.stop();
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
        String input = "https://example.com";
        ProcessResult result = orchestratorTriggerRun(input, "require-cache", version, false);
        assertExitFailure(result, "Expected require-cache to fail on a cold cache");

        UUID docId = stableDocId(input);
        String rawContentHash = rawContentHashFor(input, docId);
        String key = cacheKeyForParsedDocument(version, rawContentHash);
        assertRedisKeyState(key, false, "Expected require-cache not to write on a cold cache");
    }

    @Test
    void preferCacheWarmsCacheAndRequireCacheSucceeds() throws Exception {
        String version = "warm-" + UUID.randomUUID();
        ProcessResult warm = orchestratorTriggerRun("https://example.com", "prefer-cache", version, false);
        assertExitSuccess(warm, "Expected prefer-cache run to succeed");

        ProcessResult require = orchestratorTriggerRun("https://example.com", "require-cache", version, false);
        assertExitSuccess(require, "Expected require-cache to succeed after warm cache");
    }

    @Test
    void versionTagIsolatesReplay() throws Exception {
        String versionA = "replay-" + UUID.randomUUID();
        String versionB = "rewind-" + UUID.randomUUID();

        ProcessResult warm = orchestratorTriggerRun("https://example.com", "prefer-cache", versionA, false);
        assertExitSuccess(warm, "Expected prefer-cache run to succeed");

        ProcessResult requireSame = orchestratorTriggerRun("https://example.com", "require-cache", versionA, false);
        assertExitSuccess(requireSame, "Expected require-cache to succeed for same version");

        String input = "https://example.com";
        ProcessResult requireOther = orchestratorTriggerRun(input, "require-cache", versionB, false);
        assertExitFailure(requireOther, "Expected require-cache to fail for a new version tag");

        UUID docId = stableDocId(input);
        String rawContentHash = rawContentHashFor(input, docId);
        String key = cacheKeyForParsedDocument(versionB, rawContentHash);
        assertRedisKeyState(key, false, "Expected require-cache not to write for a new version tag");
    }

    @Test
    void bypassCacheDoesNotWarmCache() throws Exception {
        String version = "bypass-" + UUID.randomUUID();
        ProcessResult bypass = orchestratorTriggerRun("https://example.com", "bypass-cache", version, false);
        assertExitSuccess(bypass, "Expected bypass-cache run to succeed");

        UUID docId = stableDocId("https://example.com");
        String rawContentHash = rawContentHashFor("https://example.com", docId);
        String key = cacheKeyForParsedDocument(version, rawContentHash);
        assertRedisKeyState(key, false, "Expected bypass-cache not to warm cache");
    }

    @Test
    void cacheOnlyWarmsCache() throws Exception {
        String version = "cache-only-" + UUID.randomUUID();
        ProcessResult cacheOnly = orchestratorTriggerRun("https://example.com", "cache-only", version, false);
        assertExitSuccess(cacheOnly, "Expected cache-only run to succeed");

        UUID docId = stableDocId("https://example.com");
        String rawContentHash = rawContentHashFor("https://example.com", docId);
        String key = cacheKeyForParsedDocument(version, rawContentHash);
        assertRedisKeyState(key, true, "Expected cache-only to warm cache");
    }

    @Test
    void persistenceWritesOutputs() throws Exception {
        String version = "persist-" + UUID.randomUUID();
        ProcessResult result = orchestratorTriggerRun("https://example.com", "prefer-cache", version, false);
        assertExitSuccess(result, "Expected pipeline run to succeed");

        assertTrue(countRows("rawdocument") > 0, "Expected RawDocument persistence");
        assertTrue(countRows("parseddocument") > 0, "Expected ParsedDocument persistence");
        assertTrue(countRows("tokenbatch") > 0, "Expected TokenBatch persistence");
        assertTrue(countRows("indexack") > 0, "Expected IndexAck persistence");
    }

    @Test
    void invalidationClearsDownstreamCacheEntry() throws Exception {
        String input = "https://example.com";
        String version = "invalidate-" + UUID.randomUUID();

        ProcessResult warm = orchestratorTriggerRun(input, "prefer-cache", version, false);
        assertExitSuccess(warm, "Expected prefer-cache run to succeed");

        UUID docId = stableDocId(input);
        String rawContentHash = rawContentHashFor(input, docId);
        String key = cacheKeyForParsedDocument(version, rawContentHash);
        assertRedisKeyState(key, true, "Expected ParsedDocument cache entry to exist");

        invalidateParsedDocument(docId, rawContentHash, version);

        assertRedisKeyState(key, false, "Expected ParsedDocument cache entry to be removed");
    }

    @Test
    void replayHeaderTriggersConfiguredInvalidation() throws Exception {
        String input = "https://example.com";
        String version = "replay-" + UUID.randomUUID();

        ProcessResult warm = orchestratorTriggerRun(input, "prefer-cache", version, false);
        assertExitSuccess(warm, "Expected prefer-cache run to succeed");

        UUID docId = stableDocId(input);
        String rawContentHash = rawContentHashFor(input, docId);
        String key = cacheKeyForParsedDocument(version, rawContentHash);
        assertRedisKeyState(key, true, "Expected ParsedDocument cache entry to exist");

        ProcessResult replay = orchestratorTriggerRun(input, "prefer-cache", version, true);
        assertExitSuccess(replay, "Expected replay run to succeed");

        assertRedisKeyState(key, false, "Expected ParsedDocument cache entry to be removed by replay invalidation");
    }

    private ProcessResult orchestratorTriggerRun(
        String input,
        String cachePolicy,
        String versionTag,
        boolean replay
    ) throws Exception {
        String url = "http://" + orchestratorService.getHost() + ":" +
            orchestratorService.getMappedPort(8080) + "/pipeline/run";
        String payload = "{\"docId\":\"" + stableDocId(input) + "\",\"sourceUrl\":\"" + input + "\"}";
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("x-pipeline-version", versionTag)
            .header("x-pipeline-cache-policy", cachePolicy)
            .POST(HttpRequest.BodyPublishers.ofString(payload));
        if (replay) {
            builder.header("x-pipeline-replay", "true");
        }
        HttpResponse<String> response = insecureHttpClient().send(builder.build(), HttpResponse.BodyHandlers.ofString());
        String output = response.body();
        LOG.infof("Orchestrator response:%n%s", output);
        return new ProcessResult(response.statusCode() >= 200 && response.statusCode() < 300 ? 0 : 1, output);
    }

    private int countRows(String table) throws SQLException {
        Container.ExecResult result;
        try {
            result = postgres.execInContainer(
                "psql",
                "-t",
                "-A",
                "-U",
                postgres.getUsername(),
                "-d",
                postgres.getDatabaseName(),
                "-c",
                "select count(*) from " + table);
        } catch (Exception e) {
            throw new SQLException("Failed to query row count for " + table, e);
        }
        if (result.getExitCode() != 0) {
            throw new SQLException("Count query failed for " + table + ": " + result.getStderr());
        }
        String output = result.getStdout();
        String stderr = result.getStderr();
        String trimmed = (output == null ? "" : output).trim();
        if (trimmed.isBlank() && stderr != null && !stderr.isBlank()) {
            throw new SQLException("Unexpected count output for " + table + ": " + stderr.trim());
        }
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("(\\d+)").matcher(trimmed);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        throw new SQLException("Unexpected count output for " + table + ": " + trimmed);
    }

    private void assertExitSuccess(ProcessResult result, String message) {
        assertTrue(result.exitCode == 0, message + ": " + result.output);
    }

    private void assertExitFailure(ProcessResult result, String message) {
        assertTrue(result.exitCode != 0, message + ": " + result.output);
    }

    private void invalidateParsedDocument(UUID docId, String rawContentHash, String versionTag) throws Exception {
        String url = "http://" + cacheInvalidationService.getHost() + ":" +
            cacheInvalidationService.getMappedPort(8080) +
            "/api/v1/cache-invalidate-parsed-document-side-effect/process";
        String payload = "{\"docId\":\"" + docId + "\",\"rawContentHash\":\"" + rawContentHash + "\"}";
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
            .header("Content-Type", "application/json")
            .header("x-pipeline-version", versionTag)
            .header("x-pipeline-replay", "true")
            .POST(HttpRequest.BodyPublishers.ofString(payload))
            .build();
        HttpResponse<String> response = insecureHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        assertTrue(response.statusCode() >= 200 && response.statusCode() < 300,
            "Expected cache invalidation to succeed, got status " + response.statusCode() + ": " + response.body());
    }

    private void assertRedisKeyState(String key, boolean expected, String message) throws Exception {
        boolean exists = false;
        for (int i = 0; i < 10; i++) {
            exists = redisKeyExists(key);
            if (exists == expected) {
                break;
            }
            Thread.sleep(200);
        }
        assertTrue(exists == expected, message);
    }

    private boolean redisKeyExists(String key) throws Exception {
        Container.ExecResult result = redis.execInContainer("redis-cli", "exists", CACHE_PREFIX + key);
        return "1".equals(result.getStdout().trim());
    }

    private String cacheKeyForParsedDocument(String versionTag, String rawContentHash) {
        return versionTag + ":" + ParsedDocument.class.getName() + ":" + rawContentHash;
    }

    private UUID stableDocId(String input) {
        return UUID.nameUUIDFromBytes(input.getBytes(StandardCharsets.UTF_8));
    }

    private String rawContentHashFor(String input, UUID docId) {
        String rawContent = buildRawContent(input, docId);
        return HashingUtils.sha256Base64Url(rawContent);
    }

    private String buildRawContent(String sourceUrl, UUID docId) {
        return "Title: Example content for " + sourceUrl + "\n"
            + "DocId: " + docId + "\n"
            + "Body: This is a simulated crawl result with headers, metadata, and content.";
    }

    private HttpClient insecureHttpClient() throws Exception {
        TrustManager[] trustAll = new TrustManager[] { new X509TrustManager() {
            @Override
            public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
            }

            @Override
            public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
            }

            @Override
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return new java.security.cert.X509Certificate[0];
            }
        }};
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustAll, new SecureRandom());
        SSLParameters sslParameters = new SSLParameters();
        sslParameters.setEndpointIdentificationAlgorithm("");
        return HttpClient.newBuilder()
            .sslContext(sslContext)
            .sslParameters(sslParameters)
            .build();
    }

    private record ProcessResult(int exitCode, String output) {
    }
}
