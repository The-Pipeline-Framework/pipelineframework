package org.pipelineframework.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineYamlJpaQuery;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

@Testcontainers(disabledWithoutDocker = true)
class DynamoQueryCaptureStoreIT {
    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:3.8"))
        .withServices(LocalStackContainer.Service.DYNAMODB);

    private static DynamoDbClient dynamo;
    private String tableName;
    private DynamoQueryCaptureStore store;

    @BeforeAll
    static void startClient() {
        dynamo = DynamoDbClient.builder()
            .endpointOverride(URI.create(
                LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString()))
            .region(Region.of(LOCALSTACK.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
            .build();
    }

    @AfterAll
    static void closeClient() {
        if (dynamo != null) {
            dynamo.close();
        }
    }

    @AfterEach
    void closeStore() {
        PipelineExecutionContextHolder.clear();
        if (store != null) {
            store.close();
        }
    }

    @BeforeEach
    void createTable() {
        tableName = "query-capture-" + UUID.randomUUID();
        dynamo.createTable(CreateTableRequest.builder()
            .tableName(tableName)
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName(DynamoQueryCaptureStore.CAPTURE_KEY)
                    .attributeType(ScalarAttributeType.S)
                    .build(),
                AttributeDefinition.builder()
                    .attributeName(DynamoQueryCaptureStore.REVISION)
                    .attributeType(ScalarAttributeType.N)
                    .build())
            .keySchema(
                KeySchemaElement.builder()
                    .attributeName(DynamoQueryCaptureStore.CAPTURE_KEY)
                    .keyType(KeyType.HASH)
                    .build(),
                KeySchemaElement.builder()
                    .attributeName(DynamoQueryCaptureStore.REVISION)
                    .keyType(KeyType.RANGE)
                    .build())
            .provisionedThroughput(ProvisionedThroughput.builder()
                .readCapacityUnits(10L)
                .writeCapacityUnits(10L)
                .build())
            .build());
        dynamo.waiter().waitUntilTableExists(request -> request.tableName(tableName));
        store = new DynamoQueryCaptureStore(dynamo, tableName);
    }

    @Test
    void freshStoreReplaysFoundAndNotFoundWithoutPersistingInput() {
        QueryCaptureRecord found = found(
            "found-key", "customer prompt with api-key and hidden model reasoning", new TestOutput("safe"));
        QueryCaptureRecord notFound = new QueryCaptureRecord(
            "tenant", "execution", 2, "customer.find", "v1", "missing-key", "private selector",
            "", "", Instant.ofEpochMilli(20), QueryCaptureStatus.NOT_FOUND, "customer-missing");
        store.putIfAbsent(found).toCompletableFuture().join();
        store.putIfAbsent(notFound).toCompletableFuture().join();

        DynamoQueryCaptureStore restarted = new DynamoQueryCaptureStore(dynamo, tableName);
        QueryCaptureRecord replayed = restarted.get("found-key").toCompletableFuture().join().orElseThrow();
        QueryCaptureRecord replayedMissing = restarted.get("missing-key").toCompletableFuture().join().orElseThrow();

        assertEquals(found.outputJson(), replayed.outputJson());
        assertEquals(QueryCaptureStatus.NOT_FOUND, replayedMissing.status());
        assertTrue(replayed.inputJson().startsWith("sha256:"));
        String persisted = dynamo.scan(ScanRequest.builder().tableName(tableName).build()).items().toString();
        assertFalse(persisted.contains("customer prompt"));
        assertFalse(persisted.contains("api-key"));
        assertFalse(persisted.contains("hidden model reasoning"));
        assertFalse(persisted.contains("private selector"));
    }

    @Test
    void freshRuntimeReplaysCaptureWithoutRedispatchingProvider() {
        CountingConnector connector = new CountingConnector();
        QueryStepDescriptor descriptor = descriptor();
        PipelineExecutionContext context = new PipelineExecutionContext("tenant", "execution", 2);
        PipelineExecutionContextHolder.set(context);
        QueryStepSupport firstRuntime = new QueryStepSupport(List.of(connector), List.of(store));

        TestOutput first = firstRuntime.queryOneToOne(
            descriptor, new TestInput("customer-7"), TestOutput.class).await().atMost(Duration.ofSeconds(5));

        PipelineExecutionContextHolder.set(context);
        QueryStepSupport restartedRuntime = new QueryStepSupport(
            List.of(connector), List.of(new DynamoQueryCaptureStore(dynamo, tableName)));
        TestOutput replayed = restartedRuntime.queryOneToOne(
            descriptor, new TestInput("customer-7"), TestOutput.class).await().atMost(Duration.ofSeconds(5));

        assertEquals(first, replayed);
        assertEquals(1, connector.calls.get());
    }

    @Test
    void concurrentUnaryWritersConvergeOnOneAuthority() {
        QueryCaptureRecord first = found("race-key", "input", new TestOutput("first"));
        QueryCaptureRecord second = found("race-key", "input", new TestOutput("second"));

        List<QueryCaptureRecord> results = List.of(
            CompletableFuture.supplyAsync(() -> store.putIfAbsent(first).toCompletableFuture().join()),
            CompletableFuture.supplyAsync(() -> store.putIfAbsent(second).toCompletableFuture().join()))
            .stream().map(CompletableFuture::join).toList();

        assertEquals(results.get(0).outputJson(), results.get(1).outputJson());
        assertEquals(results.get(0).outputJson(), store.get("race-key").toCompletableFuture().join()
            .orElseThrow().outputJson());
    }

    @Test
    void streamingCommitIncludingEmptyObservationSurvivesRestart() {
        StreamingQueryCaptureRequest request = request("stream-key");
        StreamingQueryCaptureWriter writer = assertInstanceOf(
            StreamingQueryCaptureOpen.Write.class,
            store.openStreaming(request).toCompletableFuture().join()).writer();
        writer.append(item(0, "first")).toCompletableFuture().join();
        writer.append(item(1, "second")).toCompletableFuture().join();
        writer.commit().toCompletableFuture().join();

        DynamoQueryCaptureStore restarted = new DynamoQueryCaptureStore(dynamo, tableName);
        StreamingQueryCaptureOpen.Replay replay = assertInstanceOf(
            StreamingQueryCaptureOpen.Replay.class,
            restarted.openStreaming(request).toCompletableFuture().join());
        assertEquals(List.of(item(0, "first"), item(1, "second")), collectOneAtATime(replay.items()));

        StreamingQueryCaptureRequest emptyRequest = request("empty-key");
        StreamingQueryCaptureWriter emptyWriter = assertInstanceOf(
            StreamingQueryCaptureOpen.Write.class,
            restarted.openStreaming(emptyRequest).toCompletableFuture().join()).writer();
        emptyWriter.commit().toCompletableFuture().join();
        StreamingQueryCaptureOpen.Replay emptyReplay = assertInstanceOf(
            StreamingQueryCaptureOpen.Replay.class,
            new DynamoQueryCaptureStore(dynamo, tableName)
                .openStreaming(emptyRequest).toCompletableFuture().join());
        assertEquals(List.of(), collect(emptyReplay.items()));
    }

    @Test
    void oneStreamingWriterWinsAndWaiterReplaysItsCommit() {
        StreamingQueryCaptureRequest request = request("claim-key");
        StreamingQueryCaptureWriter winner = assertInstanceOf(
            StreamingQueryCaptureOpen.Write.class,
            store.openStreaming(request).toCompletableFuture().join()).writer();
        DynamoQueryCaptureStore contender = new DynamoQueryCaptureStore(
            dynamo, tableName, Duration.ofSeconds(5), Duration.ofMillis(10));
        CompletableFuture<StreamingQueryCaptureOpen> waiting = contender.openStreaming(request).toCompletableFuture();

        assertFalse(waiting.isDone());
        winner.append(item(0, "winner")).toCompletableFuture().join();
        winner.commit().toCompletableFuture().join();

        StreamingQueryCaptureOpen.Replay replay = assertInstanceOf(
            StreamingQueryCaptureOpen.Replay.class, waiting.join());
        assertEquals(List.of(item(0, "winner")), collect(replay.items()));
    }

    @Test
    void expiredWriterIsReclaimedAndItsPartialRowsAreIgnored() throws Exception {
        StreamingQueryCaptureRequest request = request("reclaim-key");
        DynamoQueryCaptureStore crashed = new DynamoQueryCaptureStore(
            dynamo, tableName, Duration.ofMillis(120), Duration.ofMillis(10));
        StreamingQueryCaptureWriter stale = assertInstanceOf(
            StreamingQueryCaptureOpen.Write.class,
            crashed.openStreaming(request).toCompletableFuture().join()).writer();
        stale.append(item(0, "partial")).toCompletableFuture().join();
        crashed.close();
        Thread.sleep(180L);

        DynamoQueryCaptureStore restarted = new DynamoQueryCaptureStore(
            dynamo, tableName, Duration.ofMillis(120), Duration.ofMillis(10));
        StreamingQueryCaptureWriter recovered = assertInstanceOf(
            StreamingQueryCaptureOpen.Write.class,
            restarted.openStreaming(request).toCompletableFuture().join()).writer();
        recovered.append(item(0, "authoritative")).toCompletableFuture().join();
        recovered.commit().toCompletableFuture().join();

        StreamingQueryCaptureOpen.Replay replay = assertInstanceOf(
            StreamingQueryCaptureOpen.Replay.class,
            restarted.openStreaming(request).toCompletableFuture().join());
        assertEquals(List.of(item(0, "authoritative")), collect(replay.items()));
        CompletionException staleFailure = assertThrows(CompletionException.class,
            () -> stale.append(item(1, "stale")).toCompletableFuture().join());
        assertInstanceOf(QueryCaptureStoreException.class, staleFailure.getCause());
    }

    @Test
    void tombstoneAndMaintenanceClearHideCapturedAuthority() {
        store.putIfAbsent(found("remove-key", "input", new TestOutput("safe"))).toCompletableFuture().join();
        assertTrue(store.remove("remove-key").toCompletableFuture().join());
        assertTrue(store.get("remove-key").toCompletableFuture().join().isEmpty());

        store.putIfAbsent(found("clear-key", "input", new TestOutput("safe"))).toCompletableFuture().join();
        store.clear().toCompletableFuture().join();
        assertTrue(store.get("clear-key").toCompletableFuture().join().isEmpty());
    }

    @Test
    void staleAbortCannotSupersedeAStreamingTombstone() {
        StreamingQueryCaptureRequest request = request("removed-stream-key");
        StreamingQueryCaptureWriter writer = assertInstanceOf(
            StreamingQueryCaptureOpen.Write.class,
            store.openStreaming(request).toCompletableFuture().join()).writer();

        assertTrue(store.remove(request.captureKey()).toCompletableFuture().join());
        writer.abort().toCompletableFuture().join();

        assertFalse(store.remove(request.captureKey()).toCompletableFuture().join());
        assertInstanceOf(
            StreamingQueryCaptureOpen.Write.class,
            store.openStreaming(request).toCompletableFuture().join());
    }

    @Test
    void failedWriterActionCanStillAbortAndReleaseTheCapture() {
        StreamingQueryCaptureRequest request = request("failed-writer-key");
        StreamingQueryCaptureWriter writer = assertInstanceOf(
            StreamingQueryCaptureOpen.Write.class,
            store.openStreaming(request).toCompletableFuture().join()).writer();

        assertThrows(CompletionException.class,
            () -> writer.append(item(1, "wrong-ordinal")).toCompletableFuture().join());
        writer.abort().toCompletableFuture().join();

        assertInstanceOf(
            StreamingQueryCaptureOpen.Write.class,
            store.openStreaming(request).toCompletableFuture().join());
    }

    private static QueryCaptureRecord found(String key, String input, TestOutput output) {
        return new QueryCaptureRecord(
            "tenant", "execution", 2, "customer.find", "v1", key, input,
            "{\"value\":\"" + output.value() + "\"}", TestOutput.class.getName(),
            Instant.ofEpochMilli(10), QueryCaptureStatus.FOUND, "found");
    }

    private static StreamingQueryCaptureRequest request(String key) {
        return new StreamingQueryCaptureRequest(
            "tenant", "execution", 2, "customer.find.many", "v1", key,
            "private prompt", TestOutput.class.getName());
    }

    private static QueryStepDescriptor descriptor() {
        return new QueryStepDescriptor(
            "LoadCustomer",
            "customer.find",
            "fixture",
            "v1",
            TestInput.class.getName(),
            TestOutput.class.getName(),
            "ONE_TO_ONE",
            List.of("id"),
            new PipelineYamlJpaQuery(
                "example.CustomerEntity",
                Map.of("id", "input.id"),
                Map.of("value", "value"),
                "single"));
    }

    private static StreamingQueryCaptureItem item(long ordinal, String value) {
        return new StreamingQueryCaptureItem(
            ordinal, "{\"value\":\"" + value + "\"}", TestOutput.class.getName());
    }

    private static List<StreamingQueryCaptureItem> collect(
        Flow.Publisher<StreamingQueryCaptureItem> publisher
    ) {
        CompletableFuture<List<StreamingQueryCaptureItem>> result = new CompletableFuture<>();
        List<StreamingQueryCaptureItem> items = new ArrayList<>();
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(StreamingQueryCaptureItem item) {
                items.add(item);
            }

            @Override
            public void onError(Throwable failure) {
                result.completeExceptionally(failure);
            }

            @Override
            public void onComplete() {
                result.complete(List.copyOf(items));
            }
        });
        return result.orTimeout(5, java.util.concurrent.TimeUnit.SECONDS).join();
    }

    private static List<StreamingQueryCaptureItem> collectOneAtATime(
        Flow.Publisher<StreamingQueryCaptureItem> publisher
    ) {
        CompletableFuture<List<StreamingQueryCaptureItem>> result = new CompletableFuture<>();
        List<StreamingQueryCaptureItem> items = new ArrayList<>();
        publisher.subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;

            @Override
            public void onSubscribe(Flow.Subscription value) {
                subscription = value;
                subscription.request(1L);
            }

            @Override
            public void onNext(StreamingQueryCaptureItem item) {
                items.add(item);
                subscription.request(1L);
            }

            @Override
            public void onError(Throwable failure) {
                result.completeExceptionally(failure);
            }

            @Override
            public void onComplete() {
                result.complete(List.copyOf(items));
            }
        });
        return result.orTimeout(5, java.util.concurrent.TimeUnit.SECONDS).join();
    }

    record TestOutput(String value) {
    }

    record TestInput(String id) {
    }

    private static final class CountingConnector implements FrameworkQueryConnector {
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public String connectorName() {
            return "fixture";
        }

        @Override
        public <O> java.util.concurrent.CompletionStage<O> queryOne(
            QueryRequest<?> request,
            Class<O> outputType
        ) {
            return CompletableFuture.completedFuture(outputType.cast(
                new TestOutput("provider-" + calls.incrementAndGet())));
        }
    }
}
