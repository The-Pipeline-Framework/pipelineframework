/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.search.common.dto.CrawlRequestDto;
import org.pipelineframework.telemetry.PipelineExecutionEvent;
import org.pipelineframework.telemetry.PipelineReplayDocument;
import org.pipelineframework.telemetry.PipelineReplayTopology;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SearchReplayEndToEndIT {

    private static final Logger LOG = Logger.getLogger(SearchReplayEndToEndIT.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Network NETWORK = Network.newNetwork();
    private static final Path DEV_CERTS_DIR =
        Paths.get(System.getProperty("user.dir"))
            .resolve("../target/dev-certs")
            .normalize()
            .toAbsolutePath();
    private static final Path TEST_E2E_DIR =
        Paths.get(System.getProperty("user.dir"), "target", "test-e2e").toAbsolutePath().normalize();
    private static final Path REPLAY_CAPTURE_DIR = TEST_E2E_DIR.resolve("replay").resolve("search-runs");
    private static final Path WARM_CACHE_REPLAY_FILE =
        TEST_E2E_DIR.resolve("replay").resolve("search-warm-cache-replay.json");
    private static final Path CACHE_HIT_REPLAY_FILE =
        TEST_E2E_DIR.resolve("replay").resolve("search-cache-hit-replay.json");
    private static final String REPLAY_DIR_IN_CONTAINER = "/work/replay";
    private static final String CONTAINER_KEYSTORE_PATH = "/deployments/server-keystore.jks";
    private static final String CONTAINER_TRUSTSTORE_PATH = "/deployments/client-truststore.jks";
    private static final int URL_COUNT = Integer.getInteger("search.replay.synthetic-url-count", 100);

    private static final String CRAWL_IMAGE = System.getProperty(
        "search.image.crawl-source", "localhost/search-pipeline/crawl-source-svc:latest");
    private static final String PARSE_IMAGE = System.getProperty(
        "search.image.parse-document", "localhost/search-pipeline/parse-document-svc:latest");
    private static final String TOKENIZE_IMAGE = System.getProperty(
        "search.image.tokenize-content", "localhost/search-pipeline/tokenize-content-svc:latest");
    private static final String EMBED_IMAGE = System.getProperty(
        "search.image.embed-content", "localhost/search-pipeline/embed-content-svc:latest");
    private static final String INDEX_IMAGE = System.getProperty(
        "search.image.index-document", "localhost/search-pipeline/index-document-svc:latest");
    private static final String ORCHESTRATOR_IMAGE = System.getProperty(
        "search.image.orchestrator", "localhost/search-pipeline/orchestrator-svc:latest");
    private static final String PERSISTENCE_IMAGE = System.getProperty(
        "search.image.persistence", "localhost/search-pipeline/persistence-svc:latest");
    private static final String CACHE_INVALIDATION_IMAGE = System.getProperty(
        "search.image.cache-invalidation", "localhost/search-pipeline/cache-invalidation-svc:latest");

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
            .waitingFor(Wait.forHttp("/q/health").forPort(8080).withStartupTimeout(Duration.ofSeconds(60)));

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
            .waitingFor(Wait.forHttp("/q/health").forPort(8080).withStartupTimeout(Duration.ofSeconds(60)));

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
            .waitingFor(Wait.forHttp("/q/health").forPort(8080).withStartupTimeout(Duration.ofSeconds(60)));

    private static final GenericContainer<?> embedService =
        new GenericContainer<>(EMBED_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("embed-content-svc")
            .withFileSystemBind(
                DEV_CERTS_DIR.resolve("embed-content-svc/server-keystore.jks").toString(),
                CONTAINER_KEYSTORE_PATH,
                BindMode.READ_ONLY)
            .withExposedPorts(8080)
            .withEnv("QUARKUS_PROFILE", "test")
            .withEnv("SERVER_KEYSTORE_PATH", CONTAINER_KEYSTORE_PATH)
            .withEnv("QUARKUS_HTTP_INSECURE_REQUESTS", "enabled")
            .withEnv("QUARKUS_HTTP_PORT", "8080")
            .withEnv("SEARCH_EMBED_DELAY_MS", System.getProperty("search.replay.embed.delay-ms", "25"))
            .withEnv("SEARCH_EMBED_VECTOR_VERSION", System.getProperty("search.replay.embed.vector-version", "demo-v1"))
            .waitingFor(Wait.forHttp("/q/health").forPort(8080).withStartupTimeout(Duration.ofSeconds(60)));

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
            .waitingFor(Wait.forHttp("/q/health").forPort(8080).withStartupTimeout(Duration.ofSeconds(60)));

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
            .waitingFor(Wait.forHttp("/q/health").forPort(8080).withStartupTimeout(Duration.ofSeconds(60)));

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
            .waitingFor(Wait.forHttp("/q/health").forPort(8080).withStartupTimeout(Duration.ofSeconds(60)));

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
            .withFileSystemBind(REPLAY_CAPTURE_DIR.toString(), REPLAY_DIR_IN_CONTAINER, BindMode.READ_WRITE)
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
            .withEnv("QUARKUS_REST_CLIENT_PROCESS_EMBED_CONTENT_URL", "http://embed-content-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_PROCESS_INDEX_DOCUMENT_URL", "http://index-document-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_ORCHESTRATOR_SERVICE_URL", "http://orchestrator-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_PERSISTENCE_RAW_DOCUMENT_SIDE_EFFECT_URL", "http://persistence-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_PERSISTENCE_PARSED_DOCUMENT_SIDE_EFFECT_URL", "http://persistence-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_PERSISTENCE_TOKEN_BATCH_SIDE_EFFECT_URL", "http://persistence-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_PERSISTENCE_EMBEDDED_CHUNK_SIDE_EFFECT_URL", "http://persistence-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_PERSISTENCE_INDEX_ACK_SIDE_EFFECT_URL", "http://persistence-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_RAW_DOCUMENT_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_PARSED_DOCUMENT_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_TOKEN_BATCH_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_EMBEDDED_CHUNK_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_INDEX_ACK_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_INVALIDATE_CRAWL_REQUEST_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_INVALIDATE_RAW_DOCUMENT_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_INVALIDATE_PARSED_DOCUMENT_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_INVALIDATE_TOKEN_BATCH_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_INVALIDATE_ALL_CRAWL_REQUEST_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_INVALIDATE_ALL_RAW_DOCUMENT_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_INVALIDATE_ALL_PARSED_DOCUMENT_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_REST_CLIENT_OBSERVE_CACHE_INVALIDATE_ALL_TOKEN_BATCH_SIDE_EFFECT_URL", "http://cache-invalidation-svc:8080")
            .withEnv("QUARKUS_OTEL_ENABLED", "true")
            .withEnv("QUARKUS_OTEL_SDK_DISABLED", "false")
            .withEnv("QUARKUS_OTEL_METRICS_ENABLED", "false")
            .withEnv("QUARKUS_OTEL_TRACES_ENABLED", "true")
            .withEnv("QUARKUS_OTEL_LOGS_ENABLED", "false")
            .withEnv("QUARKUS_OTEL_EXPORTER_OTLP_ENABLED", "false")
            .withEnv("QUARKUS_OTEL_TRACES_SAMPLER", "parentbased_always_on")
            .withEnv("QUARKUS_OTEL_TRACES_SAMPLER_ARG", "1.0")
            .withEnv("PIPELINE_TELEMETRY_ENABLED", "true")
            .withEnv("PIPELINE_TELEMETRY_TRACING_ENABLED", "true")
            .withEnv("PIPELINE_TELEMETRY_TRACING_PER_ITEM", "true")
            .withEnv("PIPELINE_TELEMETRY_REPLAY_ENABLED", "true")
            .withEnv("PIPELINE_TELEMETRY_REPLAY_EXPORTER", "file")
            .withEnv("PIPELINE_TELEMETRY_REPLAY_FILE_PATH", REPLAY_DIR_IN_CONTAINER)
            .waitingFor(Wait.forHttp("/q/health").forPort(8080).withStartupTimeout(Duration.ofSeconds(90)));

    @BeforeAll
    static void startServices() throws IOException {
        Files.createDirectories(REPLAY_CAPTURE_DIR);
        Files.createDirectories(WARM_CACHE_REPLAY_FILE.getParent());
        Files.createDirectories(CACHE_HIT_REPLAY_FILE.getParent());
        clearReplayDirectory();
        Startables.deepStart(java.util.stream.Stream.of(
            postgres,
            redis,
            crawlService,
            parseService,
            tokenizeService,
            embedService,
            indexService,
            persistenceService,
            cacheInvalidationService,
            orchestratorService))
            .join();
    }

    @AfterAll
    static void stopServices() {
        orchestratorService.stop();
        cacheInvalidationService.stop();
        persistenceService.stop();
        indexService.stop();
        embedService.stop();
        tokenizeService.stop();
        parseService.stop();
        crawlService.stop();
        redis.stop();
        postgres.stop();
        NETWORK.close();
    }

    @Test
    void capturesWarmCacheAndCacheHitReplayDatasets() throws Exception {
        clearReplayDirectory();
        Files.deleteIfExists(WARM_CACHE_REPLAY_FILE);
        Files.deleteIfExists(CACHE_HIT_REPLAY_FILE);

        List<String> urls = syntheticUrls(URL_COUNT);
        String versionTag = "replay-" + UUID.randomUUID();
        PipelineReplayDocument warmCache = runPhase(urls, versionTag, WARM_CACHE_REPLAY_FILE);
        PipelineReplayDocument cacheHit = runPhase(urls, versionTag, CACHE_HIT_REPLAY_FILE);

        assertTrue(Files.isRegularFile(WARM_CACHE_REPLAY_FILE));
        assertTrue(Files.isRegularFile(CACHE_HIT_REPLAY_FILE));
        assertEquals("completed", warmCache.status());
        assertEquals("completed", cacheHit.status());
        assertFalse(warmCache.events().isEmpty(), "Expected warm-cache replay events");
        assertFalse(cacheHit.events().isEmpty(), "Expected cache-hit replay events");
        assertTrue(warmCache.topology().steps().stream().filter(PipelineReplayTopology.Step::sideEffect).count() >= 3,
            "Expected plugin nodes in warm-cache topology");
        assertTrue(hasAnyStep(warmCache, "Embed Content", "EmbedContent", "ProcessEmbedContent"),
            "Expected Embed Content in warm-cache replay topology");
        assertTrue(hasAnyPrimaryTransition(warmCache,
            List.of("Tokenize Content", "TokenizeContent", "ProcessTokenizeContent"),
            List.of("Embed Content", "EmbedContent", "ProcessEmbedContent")),
            "Expected Tokenize Content -> Embed Content replay transition");
        assertTrue(hasAnyPrimaryTransition(warmCache,
            List.of("Embed Content", "EmbedContent", "ProcessEmbedContent"),
            List.of("Index Document", "IndexDocument", "ProcessIndexDocument")),
            "Expected Embed Content -> Index Document replay transition");
        assertTrue(warmCache.events().stream().anyMatch(event ->
                (event.step() != null && event.step().contains("EmbedContent"))
                    || (event.service() != null && event.service().contains("ProcessEmbedContentService"))),
            "Expected direct Embed Content events in warm-cache replay");
        assertTrue(cacheHit.events().stream().anyMatch(event -> "cache_hit".equals(event.event())),
            "Expected cache-hit events in cache-hit replay");
        LOG.infof("Wrote search replay datasets to %s and %s", WARM_CACHE_REPLAY_FILE, CACHE_HIT_REPLAY_FILE);
    }

    private static boolean hasAnyStep(PipelineReplayDocument document, String... stepNames) {
        return document.topology().steps().stream()
            .anyMatch(step -> List.of(stepNames).contains(step.step()));
    }

    private static boolean hasAnyPrimaryTransition(PipelineReplayDocument document, List<String> fromSteps, List<String> toSteps) {
        return document.topology().transitions().stream()
            .anyMatch(transition ->
                fromSteps.contains(transition.from())
                    && toSteps.contains(transition.to())
                    && ("primary".equals(transition.relationKind()) || transition.relationKind() == null));
    }

    private static PipelineReplayDocument runPhase(List<String> urls, String versionTag, Path outputFile) throws Exception {
        clearReplayDirectory();
        List<ProcessResult> results = new ArrayList<>();
        for (String url : urls) {
            // Deliberately omit x-pipeline-replay; this test captures telemetry replay via the environment flag, not replay invalidation side effects.
            results.add(orchestratorTriggerRun(url, versionTag, false));
        }
        assertTrue(results.stream().allMatch(ProcessResult::success),
            () -> "Expected all replay runs to succeed but saw failures: " + results.stream()
                .filter(result -> !result.success())
                .map(ProcessResult::output)
                .limit(3)
                .toList()
                + diagnosticLogTail());

        await()
            .atMost(Duration.ofSeconds(60))
            .until(() -> countReplayFiles(REPLAY_CAPTURE_DIR) == URL_COUNT);
        assertEquals(urls.size(), countReplayFiles(REPLAY_CAPTURE_DIR));
        return mergeReplayDocuments(REPLAY_CAPTURE_DIR, outputFile);
    }

    private static ProcessResult orchestratorTriggerRun(String input, String versionTag, boolean replay) throws Exception {
        String url = "http://" + orchestratorService.getHost() + ":" +
            orchestratorService.getMappedPort(8080) + "/pipeline/run";
        String payload = OBJECT_MAPPER.writeValueAsString(CrawlRequestDto.builder()
            .docId(stableDocId(input))
            .sourceUrl(input)
            .build());
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("x-pipeline-version", versionTag)
            .header("x-pipeline-cache-policy", "prefer-cache")
            .POST(HttpRequest.BodyPublishers.ofString(payload));
        if (replay) {
            builder.header("x-pipeline-replay", "true");
        }
        HttpResponse<String> response = insecureHttpClient().send(builder.build(), HttpResponse.BodyHandlers.ofString());
        return new ProcessResult(response.statusCode() >= 200 && response.statusCode() < 300, response.body());
    }

    private static PipelineReplayDocument mergeReplayDocuments(Path replayDir, Path outputFile) throws Exception {
        List<Path> replayFiles;
        try (var files = Files.list(replayDir)) {
            replayFiles = files
                .filter(path -> path.getFileName().toString().endsWith(".json"))
                .sorted()
                .toList();
        }
        List<PipelineReplayDocument> documents = new ArrayList<>();
        for (Path replayFile : replayFiles) {
            documents.add(PipelineJson.mapper().readValue(replayFile.toFile(), PipelineReplayDocument.class));
        }
        if (documents.isEmpty()) {
            throw new IllegalStateException("No replay documents found in " + replayDir);
        }
        documents.sort(Comparator.comparing(PipelineReplayDocument::startedAt));
        PipelineReplayDocument first = documents.getFirst();
        Instant earliestStart = first.startedAt();
        List<PipelineExecutionEvent> shiftedEvents = new ArrayList<>();
        String status = "completed";
        String failureType = null;
        String failureMessage = null;
        double maxEndSeconds = 0;

        for (PipelineReplayDocument document : documents) {
            if (!"completed".equals(document.status()) && "completed".equals(status)) {
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
            if (left.startTime() != right.startTime()) {
                return Double.compare(left.startTime(), right.startTime());
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
            first.pipeline(),
            earliestStart,
            Math.round(maxEndSeconds * 1000),
            status,
            failureType,
            failureMessage,
            first.runParameters(),
            first.topology(),
            List.copyOf(renumbered));
        Files.createDirectories(outputFile.getParent());
        Files.writeString(outputFile, PipelineJson.mapper().writerWithDefaultPrettyPrinter().writeValueAsString(merged));
        return merged;
    }

    private static long countReplayFiles(Path replayDir) throws IOException {
        try (var stream = Files.list(replayDir)) {
            return stream.filter(path -> path.getFileName().toString().endsWith(".json")).count();
        }
    }

    private static String nullSafe(String value) {
        return value == null ? "" : value;
    }

    private static String diagnosticLogTail() {
        return "\n\nContainer log tails:"
            + "\n- orchestrator-svc:\n" + tailLogs(orchestratorService.getLogs(), 40)
            + "\n- crawl-source-svc:\n" + tailLogs(crawlService.getLogs(), 25)
            + "\n- parse-document-svc:\n" + tailLogs(parseService.getLogs(), 25)
            + "\n- tokenize-content-svc:\n" + tailLogs(tokenizeService.getLogs(), 25)
            + "\n- embed-content-svc:\n" + tailLogs(embedService.getLogs(), 25)
            + "\n- index-document-svc:\n" + tailLogs(indexService.getLogs(), 40)
            + "\n- persistence-svc:\n" + tailLogs(persistenceService.getLogs(), 25)
            + "\n- cache-invalidation-svc:\n" + tailLogs(cacheInvalidationService.getLogs(), 25);
    }

    private static String tailLogs(String logs, int lines) {
        if (logs == null || logs.isBlank()) {
            return "<no logs>";
        }
        String[] split = logs.split("\\R");
        int start = Math.max(0, split.length - lines);
        return String.join(System.lineSeparator(), java.util.Arrays.copyOfRange(split, start, split.length));
    }

    private static void clearReplayDirectory() throws IOException {
        if (!Files.exists(REPLAY_CAPTURE_DIR)) {
            Files.createDirectories(REPLAY_CAPTURE_DIR);
            return;
        }
        try (var stream = Files.list(REPLAY_CAPTURE_DIR)) {
            for (Path file : stream.toList()) {
                Files.deleteIfExists(file);
            }
        }
    }

    private static List<String> syntheticUrls(int count) {
        return IntStream.range(0, count)
            .mapToObj(index -> "https://example.com/search/replay/doc-" + String.format("%03d", index))
            .toList();
    }

    private static UUID stableDocId(String input) {
        return UUID.nameUUIDFromBytes(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static HttpClient insecureHttpClient() throws Exception {
        TrustManager[] trustAll = new TrustManager[] { new X509TrustManager() {
            @Override
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return new java.security.cert.X509Certificate[0];
            }

            @Override
            public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
            }

            @Override
            public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
            }
        }};

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustAll, new SecureRandom());
        SSLParameters sslParameters = new SSLParameters();
        sslParameters.setEndpointIdentificationAlgorithm("");

        return HttpClient.newBuilder()
            .sslContext(sslContext)
            .sslParameters(sslParameters)
            .connectTimeout(Duration.ofSeconds(15))
            .build();
    }

    private record ProcessResult(boolean success, String output) {
    }
}
