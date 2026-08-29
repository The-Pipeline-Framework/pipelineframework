package org.pipelineframework.query;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionCheck;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * DynamoDB Query observation authority backed by immutable, append-only revisions.
 *
 * <p>The table uses string partition key {@value #CAPTURE_KEY} and numeric sort key
 * {@value #REVISION}. The capture key is already a SHA-256 identity over tenant, execution,
 * step, Query descriptor, and selected input. Every transition consistently reads the latest
 * event and conditionally creates its successor; no mutable head or {@code UpdateItem} is used.</p>
 */
@ApplicationScoped
@IfBuildProperty(name = "pipeline.query.capture-store.provider", stringValue = "dynamo")
public class DynamoQueryCaptureStore implements QueryCaptureStore {
    public static final String CAPTURE_KEY = "capture_key";
    public static final String REVISION = "revision";
    public static final String SCHEMA_VERSION = "schema_version";
    public static final String EVENT_JSON = "event_json";
    static final String GENERATION = "generation";
    static final String OWNER_TOKEN = "owner_token";
    static final String LEASE_EXPIRES_AT = "lease_expires_at";
    public static final int MAX_EVENT_BYTES = 300 * 1024;

    private static final Duration DEFAULT_LEASE = Duration.ofMinutes(5);
    private static final Duration DEFAULT_POLL = Duration.ofMillis(250);
    private static final int REPLAY_PAGE_SIZE = 25;

    private final DynamoDbClient client;
    private final String tableName;
    private final QueryCaptureEventCodec codec;
    private final Duration leaseDuration;
    private final Duration pollInterval;
    private final Clock clock;
    private final Executor worker;
    private final AtomicBoolean running = new AtomicBoolean(true);

    @Inject
    public DynamoQueryCaptureStore(DynamoDbClient client, QueryCaptureStoreConfig config) {
        this(
            client,
            Objects.requireNonNull(config, "Query capture store config must not be null").dynamo().table(),
            config.dynamo().streamingLeaseDuration(),
            config.dynamo().streamingPollInterval());
    }

    /** Creates a store for an explicitly managed client and table. The client remains caller-owned. */
    public DynamoQueryCaptureStore(DynamoDbClient client, String tableName) {
        this(client, tableName, DEFAULT_LEASE, DEFAULT_POLL);
    }

    /** Creates a store with explicit distributed streaming timing. */
    public DynamoQueryCaptureStore(
        DynamoDbClient client,
        String tableName,
        Duration leaseDuration,
        Duration pollInterval
    ) {
        this(client, tableName, leaseDuration, pollInterval, new QueryCaptureEventCodec(), Clock.systemUTC(),
            Infrastructure.getDefaultWorkerPool());
    }

    DynamoQueryCaptureStore(
        DynamoDbClient client,
        String tableName,
        Duration leaseDuration,
        Duration pollInterval,
        QueryCaptureEventCodec codec,
        Clock clock,
        Executor worker
    ) {
        this.client = Objects.requireNonNull(client, "DynamoDB client must not be null");
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Query capture DynamoDB table must not be blank");
        }
        this.tableName = tableName;
        this.leaseDuration = positive(leaseDuration, "streaming lease duration");
        this.pollInterval = positive(pollInterval, "streaming poll interval");
        this.codec = Objects.requireNonNull(codec, "Query capture event codec must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.worker = Objects.requireNonNull(worker, "worker executor must not be null");
    }

    @Override
    public String providerName() {
        return "dynamo";
    }

    /** Stops this store's polling and lease-heartbeat work without closing the caller-owned client. */
    @PreDestroy
    public void close() {
        running.set(false);
    }

    @Override
    public CompletionStage<Optional<QueryCaptureRecord>> get(String captureKey) {
        return blocking("read Query capture", () -> {
            Optional<StoredEvent> latest = latest(captureKey);
            if (latest.isEmpty() || latest.orElseThrow().event().kind() == QueryCaptureEventCodec.Kind.TOMBSTONE) {
                return Optional.empty();
            }
            QueryCaptureEventCodec.Event event = latest.orElseThrow().event();
            if (event.kind() != QueryCaptureEventCodec.Kind.FOUND
                && event.kind() != QueryCaptureEventCodec.Kind.NOT_FOUND) {
                throw new QueryCaptureStoreException(
                    "Query capture key '" + captureKey + "' contains a streaming observation");
            }
            return Optional.of(codec.toRecord(event));
        });
    }

    @Override
    public CompletionStage<QueryCaptureRecord> putIfAbsent(QueryCaptureRecord record) {
        Objects.requireNonNull(record, "Query capture record must not be null");
        return blocking("write Query capture", () -> putUnary(record));
    }

    @Override
    public CompletionStage<StreamingQueryCaptureOpen> openStreaming(StreamingQueryCaptureRequest request) {
        Objects.requireNonNull(request, "streaming Query capture request must not be null");
        CompletableFuture<StreamingQueryCaptureOpen> result = new CompletableFuture<>();
        openStreamingAttempt(request, result);
        return result;
    }

    @Override
    public CompletionStage<Boolean> remove(String captureKey) {
        return blocking("remove Query capture", () -> {
            for (;;) {
                Optional<StoredEvent> current = latest(captureKey);
                if (current.isEmpty() || current.orElseThrow().event().kind() == QueryCaptureEventCodec.Kind.TOMBSTONE) {
                    return false;
                }
                StoredEvent authority = current.orElseThrow();
                try {
                    append(captureKey, authority.revision() + 1L, codec.tombstone(authority.event()));
                    return true;
                } catch (QueryCaptureConflictException ignored) {
                    // Re-read and either append the next tombstone or observe another remover.
                }
            }
        });
    }

    @Override
    public CompletionStage<Void> clear() {
        return blockingVoid("clear Query capture table", () -> {
            Map<String, AttributeValue> cursor = Map.of();
            do {
                ScanRequest.Builder request = ScanRequest.builder()
                    .tableName(tableName)
                    .projectionExpression("#captureKey, #revision")
                    .expressionAttributeNames(Map.of(
                        "#captureKey", CAPTURE_KEY,
                        "#revision", REVISION))
                    .consistentRead(true);
                if (!cursor.isEmpty()) {
                    request.exclusiveStartKey(cursor);
                }
                ScanResponse page = client.scan(request.build());
                deleteBatch(page.items());
                cursor = page.lastEvaluatedKey();
            } while (cursor != null && !cursor.isEmpty());
        });
    }

    private QueryCaptureRecord putUnary(QueryCaptureRecord record) {
        QueryCaptureEventCodec.Event candidate = codec.unary(record);
        for (;;) {
            Optional<StoredEvent> current = latest(record.captureKey());
            if (current.isPresent() && current.orElseThrow().event().kind() != QueryCaptureEventCodec.Kind.TOMBSTONE) {
                QueryCaptureEventCodec.Event winner = current.orElseThrow().event();
                if (winner.kind() != QueryCaptureEventCodec.Kind.FOUND
                    && winner.kind() != QueryCaptureEventCodec.Kind.NOT_FOUND) {
                    throw new QueryCaptureStoreException(
                        "Query capture key '" + record.captureKey() + "' contains a streaming observation");
                }
                return codec.toRecord(winner);
            }
            long revision = current.map(stored -> stored.revision() + 1L).orElse(0L);
            try {
                append(record.captureKey(), revision, candidate);
                return codec.toRecord(candidate);
            } catch (QueryCaptureConflictException ignored) {
                // A competing result is the authority. Re-read it.
            }
        }
    }

    private void openStreamingAttempt(
        StreamingQueryCaptureRequest request,
        CompletableFuture<StreamingQueryCaptureOpen> result
    ) {
        if (result.isDone()) {
            return;
        }
        if (!running.get()) {
            result.completeExceptionally(new QueryCaptureStoreException("Dynamo Query capture store is closed"));
            return;
        }
        blocking("open streaming Query capture", () -> decideStreamingOpen(request))
            .whenComplete((decision, failure) -> {
                if (failure != null) {
                    result.completeExceptionally(unwrap(failure));
                } else if (decision instanceof OpenDecision.Ready ready) {
                    result.complete(ready.open());
                } else {
                    delayed(() -> openStreamingAttempt(request, result));
                }
            });
    }

    private OpenDecision decideStreamingOpen(StreamingQueryCaptureRequest request) {
        for (;;) {
            Optional<StoredEvent> latest = latest(request.captureKey());
            if (latest.isPresent()) {
                StoredEvent current = latest.orElseThrow();
                QueryCaptureEventCodec.Event event = current.event();
                verifyIdentity(request, event);
                switch (event.kind()) {
                    case FOUND, NOT_FOUND -> throw new QueryCaptureStoreException(
                        "Query capture key '" + request.captureKey() + "' contains a unary observation");
                    case STREAM_COMMITTED -> {
                        return new OpenDecision.Ready(new StreamingQueryCaptureOpen.Replay(
                            new DynamoReplayPublisher(request.captureKey(), current.revision(), event)));
                    }
                    case STREAM_OPEN, STREAM_ITEM -> {
                        if (event.leaseExpiresAtEpochMs() > clock.millis()) {
                            return OpenDecision.Wait.INSTANCE;
                        }
                        return claimWriter(request, current.revision() + 1L, event.generation() + 1L);
                    }
                    case STREAM_ABORTED, TOMBSTONE -> {
                        return claimWriter(request, current.revision() + 1L, event.generation() + 1L);
                    }
                }
            }
            return claimWriter(request, 0L, 0L);
        }
    }

    private OpenDecision claimWriter(StreamingQueryCaptureRequest request, long revision, long generation) {
        String ownerToken = UUID.randomUUID().toString();
        QueryCaptureEventCodec.Event open = codec.streamOpen(
            request, generation, ownerToken, clock.millis() + leaseDuration.toMillis());
        try {
            append(request.captureKey(), revision, open);
            return new OpenDecision.Ready(new StreamingQueryCaptureOpen.Write(
                new DynamoStreamingWriter(request.captureKey(), generation, ownerToken, request.outputType())));
        } catch (QueryCaptureConflictException ignored) {
            return OpenDecision.Wait.INSTANCE;
        }
    }

    private void verifyIdentity(StreamingQueryCaptureRequest request, QueryCaptureEventCodec.Event event) {
        if (!request.tenantId().equals(event.tenantId())
            || !request.executionId().equals(event.executionId())
            || request.stepIndex() != event.stepIndex()
            || !request.queryId().equals(event.queryId())
            || !request.queryVersion().equals(event.queryVersion())
            || !request.outputType().equals(event.outputType())) {
            throw new QueryCaptureStoreException(
                "Streaming Query capture request does not match the persisted observation identity");
        }
    }

    private Optional<StoredEvent> latest(String captureKey) {
        requireText(captureKey, "capture key");
        try {
            List<Map<String, AttributeValue>> items = client.query(QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("#captureKey = :captureKey")
                .expressionAttributeNames(Map.of("#captureKey", CAPTURE_KEY))
                .expressionAttributeValues(Map.of(":captureKey", stringValue(captureKey)))
                .consistentRead(true)
                .scanIndexForward(false)
                .limit(1)
                .build()).items();
            if (items.isEmpty()) {
                return Optional.empty();
            }
            Map<String, AttributeValue> item = items.getFirst();
            int schema = Math.toIntExact(longValue(item, SCHEMA_VERSION));
            if (schema != QueryCaptureEventCodec.SCHEMA_VERSION) {
                throw new QueryCaptureStoreException(
                    "Unsupported durable Query capture item schema version " + schema);
            }
            QueryCaptureEventCodec.Event event = codec.decode(stringValue(item, EVENT_JSON));
            if (!captureKey.equals(event.captureKey())) {
                throw new QueryCaptureStoreException(
                    "Durable Query capture partition key does not match its event identity");
            }
            return Optional.of(new StoredEvent(longValue(item, REVISION), event));
        } catch (QueryCaptureStoreException failure) {
            throw failure;
        } catch (DynamoDbException failure) {
            throw new QueryCaptureStoreException(
                "Failed reading Query capture '" + captureKey + "' from DynamoDB", failure);
        }
    }

    private void append(String captureKey, long revision, QueryCaptureEventCodec.Event event) {
        String encoded = codec.encode(event);
        validateSize(encoded);
        try {
            client.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(eventItem(captureKey, revision, encoded, event))
                .conditionExpression("attribute_not_exists(#captureKey) AND attribute_not_exists(#revision)")
                .expressionAttributeNames(Map.of(
                    "#captureKey", CAPTURE_KEY,
                    "#revision", REVISION))
                .build());
        } catch (ConditionalCheckFailedException failure) {
            throw new QueryCaptureConflictException(
                "Query capture revision " + revision + " already exists for key '" + captureKey + "'", failure);
        } catch (DynamoDbException failure) {
            throw new QueryCaptureStoreException(
                "Failed writing Query capture '" + captureKey + "' to DynamoDB", failure);
        }
    }

    private void appendWriter(
        String captureKey,
        StoredEvent authority,
        QueryCaptureEventCodec.Event event
    ) {
        String encoded = codec.encode(event);
        validateSize(encoded);
        long now = clock.millis();
        try {
            client.transactWriteItems(TransactWriteItemsRequest.builder()
                .transactItems(
                    TransactWriteItem.builder().conditionCheck(ConditionCheck.builder()
                        .tableName(tableName)
                        .key(Map.of(
                            CAPTURE_KEY, stringValue(captureKey),
                            REVISION, numberValue(authority.revision())))
                        .conditionExpression(
                            "#generation = :generation AND #ownerToken = :ownerToken AND #leaseExpiresAt > :now")
                        .expressionAttributeNames(Map.of(
                            "#generation", GENERATION,
                            "#ownerToken", OWNER_TOKEN,
                            "#leaseExpiresAt", LEASE_EXPIRES_AT))
                        .expressionAttributeValues(Map.of(
                            ":generation", numberValue(authority.event().generation()),
                            ":ownerToken", stringValue(authority.event().ownerToken()),
                            ":now", numberValue(now)))
                        .build()).build(),
                    TransactWriteItem.builder().put(Put.builder()
                        .tableName(tableName)
                        .item(eventItem(captureKey, authority.revision() + 1L, encoded, event))
                        .conditionExpression("attribute_not_exists(#captureKey) AND attribute_not_exists(#revision)")
                        .expressionAttributeNames(Map.of(
                            "#captureKey", CAPTURE_KEY,
                            "#revision", REVISION))
                        .build()).build())
                .build());
        } catch (TransactionCanceledException failure) {
            throw new QueryCaptureConflictException(
                "Streaming Query capture lease is stale for key '" + captureKey + "'", failure);
        } catch (DynamoDbException failure) {
            throw new QueryCaptureStoreException(
                "Failed writing Query capture '" + captureKey + "' to DynamoDB", failure);
        }
    }

    private Map<String, AttributeValue> eventItem(
        String captureKey,
        long revision,
        String encoded,
        QueryCaptureEventCodec.Event event
    ) {
        return Map.of(
            CAPTURE_KEY, stringValue(captureKey),
            REVISION, numberValue(revision),
            SCHEMA_VERSION, numberValue(QueryCaptureEventCodec.SCHEMA_VERSION),
            EVENT_JSON, stringValue(encoded),
            GENERATION, numberValue(event.generation()),
            OWNER_TOKEN, stringValue(event.ownerToken()),
            LEASE_EXPIRES_AT, numberValue(event.leaseExpiresAtEpochMs()));
    }

    private static void validateSize(String encoded) {
        int bytes = encoded.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > MAX_EVENT_BYTES) {
            throw new QueryCaptureStoreException(
                "Durable Query capture event is " + bytes + " bytes; maximum is " + MAX_EVENT_BYTES
                    + ". Carry large content with PayloadReference.");
        }
    }

    private void deleteBatch(List<Map<String, AttributeValue>> items) {
        for (int offset = 0; offset < items.size(); offset += 25) {
            List<WriteRequest> writes = items.subList(offset, Math.min(offset + 25, items.size())).stream()
                .map(item -> WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(Map.of(
                    CAPTURE_KEY, item.get(CAPTURE_KEY),
                    REVISION, item.get(REVISION))).build()).build())
                .toList();
            if (!writes.isEmpty()) {
                Map<String, List<WriteRequest>> remaining = Map.of(tableName, writes);
                for (int attempt = 0; !remaining.isEmpty() && attempt < 10; attempt++) {
                    BatchWriteItemResponse response = client.batchWriteItem(BatchWriteItemRequest.builder()
                        .requestItems(remaining)
                        .build());
                    remaining = response.unprocessedItems();
                }
                if (!remaining.isEmpty()) {
                    throw new QueryCaptureStoreException(
                        "DynamoDB did not process all Query capture clear operations");
                }
            }
        }
    }

    private <T> CompletionStage<T> blocking(String operation, Supplier<T> action) {
        return CompletableFuture.supplyAsync(() -> {
            if (!running.get()) {
                throw new QueryCaptureStoreException("Dynamo Query capture store is closed");
            }
            try {
                return action.get();
            } catch (QueryCaptureStoreException failure) {
                throw failure;
            } catch (RuntimeException failure) {
                throw new QueryCaptureStoreException("Failed to " + operation, failure);
            }
        }, worker);
    }

    private CompletionStage<Void> blockingVoid(String operation, Runnable action) {
        return CompletableFuture.runAsync(() -> {
            if (!running.get()) {
                throw new QueryCaptureStoreException("Dynamo Query capture store is closed");
            }
            try {
                action.run();
            } catch (QueryCaptureStoreException failure) {
                throw failure;
            } catch (RuntimeException failure) {
                throw new QueryCaptureStoreException("Failed to " + operation, failure);
            }
        }, worker);
    }

    private void delayed(Runnable action) {
        CompletableFuture.delayedExecutor(pollInterval.toMillis(), TimeUnit.MILLISECONDS, worker).execute(action);
    }

    private static Duration positive(Duration value, String field) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " must be positive");
        }
        return value;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof java.util.concurrent.CompletionException
            || current instanceof java.util.concurrent.ExecutionException) && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static AttributeValue stringValue(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue numberValue(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private static String stringValue(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        if (value == null || value.s() == null) {
            throw new QueryCaptureStoreException("Durable Query capture item is missing " + name);
        }
        return value.s();
    }

    private static long longValue(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        if (value == null || value.n() == null) {
            throw new QueryCaptureStoreException("Durable Query capture item is missing " + name);
        }
        try {
            return Long.parseLong(value.n());
        } catch (NumberFormatException failure) {
            throw new QueryCaptureStoreException("Durable Query capture item has invalid " + name, failure);
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new QueryCaptureStoreException(field + " must not be blank");
        }
    }

    private final class DynamoStreamingWriter implements StreamingQueryCaptureWriter {
        private final String captureKey;
        private final long generation;
        private final String ownerToken;
        private final String outputType;
        private final AtomicLong nextOrdinal = new AtomicLong();
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicReference<Throwable> heartbeatFailure = new AtomicReference<>();
        private CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);

        private DynamoStreamingWriter(String captureKey, long generation, String ownerToken, String outputType) {
            this.captureKey = captureKey;
            this.generation = generation;
            this.ownerToken = ownerToken;
            this.outputType = outputType;
            scheduleHeartbeat();
        }

        @Override
        public CompletionStage<Void> append(StreamingQueryCaptureItem item) {
            Objects.requireNonNull(item, "streaming Query capture item must not be null");
            return enqueue(() -> {
                long expected = nextOrdinal.get();
                if (item.ordinal() != expected) {
                    throw new QueryCaptureStoreException(
                        "Streaming Query capture expected ordinal " + expected + " but received " + item.ordinal());
                }
                if (!outputType.equals(item.outputType())) {
                    throw new QueryCaptureStoreException(
                        "Streaming Query capture item type does not match its observation");
                }
                StoredEvent authority = ownedLatest();
                DynamoQueryCaptureStore.this.appendWriter(captureKey, authority, codec.streamItem(
                    authority.event(), item, clock.millis() + leaseDuration.toMillis()));
                nextOrdinal.incrementAndGet();
            });
        }

        @Override
        public CompletionStage<Void> commit() {
            return enqueue(() -> {
                StoredEvent authority = ownedLatest();
                DynamoQueryCaptureStore.this.appendWriter(captureKey, authority, codec.streamTerminal(
                    authority.event(), QueryCaptureEventCodec.Kind.STREAM_COMMITTED, nextOrdinal.get()));
                closed.set(true);
            });
        }

        @Override
        public CompletionStage<Void> abort() {
            if (closed.get()) {
                return CompletableFuture.completedFuture(null);
            }
            return enqueue(() -> {
                Optional<StoredEvent> latest = DynamoQueryCaptureStore.this.latest(captureKey);
                if (latest.isPresent()) {
                    StoredEvent authority = latest.orElseThrow();
                    if (owns(authority.event())
                        && (authority.event().kind() == QueryCaptureEventCodec.Kind.STREAM_OPEN
                            || authority.event().kind() == QueryCaptureEventCodec.Kind.STREAM_ITEM)) {
                        DynamoQueryCaptureStore.this.appendWriter(
                            captureKey,
                            authority,
                            codec.streamTerminal(
                                authority.event(), QueryCaptureEventCodec.Kind.STREAM_ABORTED, nextOrdinal.get()));
                    }
                }
                closed.set(true);
            }, true);
        }

        private synchronized CompletionStage<Void> enqueue(ThrowingAction action) {
            return enqueue(action, false);
        }

        private synchronized CompletionStage<Void> enqueue(
            ThrowingAction action,
            boolean allowAfterHeartbeatFailure
        ) {
            if (!running.get()) {
                return CompletableFuture.failedFuture(
                    new QueryCaptureStoreException("Dynamo Query capture store is closed"));
            }
            if (closed.get()) {
                return CompletableFuture.failedFuture(
                    new QueryCaptureConflictException("Streaming Query capture writer is closed"));
            }
            Throwable heartbeat = heartbeatFailure.get();
            if (heartbeat != null && !allowAfterHeartbeatFailure) {
                return CompletableFuture.failedFuture(new QueryCaptureStoreException(
                    "Streaming Query capture heartbeat failed", heartbeat));
            }
            CompletableFuture<Void> current = tail.handle((ignored, failure) -> null).thenRunAsync(() -> {
                try {
                    action.run();
                } catch (QueryCaptureStoreException failure) {
                    throw failure;
                } catch (Exception failure) {
                    throw new QueryCaptureStoreException("Streaming Query capture writer failed", failure);
                }
            }, worker);
            tail = current.handle((ignored, failure) -> null);
            return current;
        }

        private StoredEvent ownedLatest() {
            StoredEvent authority = DynamoQueryCaptureStore.this.latest(captureKey)
                .orElseThrow(() -> new QueryCaptureConflictException("Streaming Query capture disappeared"));
            if (!owns(authority.event())
                || authority.event().kind() == QueryCaptureEventCodec.Kind.STREAM_ABORTED
                || authority.event().kind() == QueryCaptureEventCodec.Kind.STREAM_COMMITTED
                || authority.event().kind() == QueryCaptureEventCodec.Kind.TOMBSTONE
                || authority.event().leaseExpiresAtEpochMs() <= clock.millis()) {
                throw new QueryCaptureConflictException("Streaming Query capture lease is stale");
            }
            return authority;
        }

        private boolean owns(QueryCaptureEventCodec.Event event) {
            return generation == event.generation() && ownerToken.equals(event.ownerToken());
        }

        private void scheduleHeartbeat() {
            long delay = Math.max(1L, leaseDuration.toMillis() / 3L);
            CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS, worker).execute(() -> {
                if (closed.get() || !running.get()) {
                    return;
                }
                enqueue(() -> {
                    StoredEvent authority = ownedLatest();
                    QueryCaptureEventCodec.Event heartbeat = authority.event().withStreamState(
                        QueryCaptureEventCodec.Kind.STREAM_OPEN, "",
                        clock.millis() + leaseDuration.toMillis(), -1L, -1L);
                    DynamoQueryCaptureStore.this.appendWriter(captureKey, authority, heartbeat);
                }).whenComplete((ignored, failure) -> {
                    if (failure == null) {
                        scheduleHeartbeat();
                    } else if (!closed.get()) {
                        heartbeatFailure.compareAndSet(null, unwrap(failure));
                    }
                });
            });
        }
    }

    private final class DynamoReplayPublisher implements Flow.Publisher<StreamingQueryCaptureItem> {
        private final String captureKey;
        private final long commitRevision;
        private final QueryCaptureEventCodec.Event committed;

        private DynamoReplayPublisher(
            String captureKey,
            long commitRevision,
            QueryCaptureEventCodec.Event committed
        ) {
            this.captureKey = captureKey;
            this.commitRevision = commitRevision;
            this.committed = committed;
        }

        @Override
        public void subscribe(Flow.Subscriber<? super StreamingQueryCaptureItem> subscriber) {
            Objects.requireNonNull(subscriber, "streaming Query replay subscriber must not be null");
            subscriber.onSubscribe(new ReplaySubscription(subscriber));
        }

        private final class ReplaySubscription implements Flow.Subscription {
            private final Flow.Subscriber<? super StreamingQueryCaptureItem> subscriber;
            private final Queue<StreamingQueryCaptureItem> buffered = new ArrayDeque<>();
            private Map<String, AttributeValue> cursor = Map.of();
            private long demand;
            private long nextOrdinal;
            private boolean sourceComplete;
            private boolean draining;
            private boolean cancelled;

            private ReplaySubscription(Flow.Subscriber<? super StreamingQueryCaptureItem> subscriber) {
                this.subscriber = subscriber;
            }

            @Override
            public synchronized void request(long count) {
                if (count <= 0L) {
                    cancelled = true;
                    subscriber.onError(new IllegalArgumentException("streaming capture replay demand must be positive"));
                    return;
                }
                demand = demand > Long.MAX_VALUE - count ? Long.MAX_VALUE : demand + count;
                scheduleDrain();
            }

            @Override
            public synchronized void cancel() {
                cancelled = true;
                buffered.clear();
            }

            private synchronized void scheduleDrain() {
                if (!draining && !cancelled) {
                    draining = true;
                    worker.execute(this::drain);
                }
            }

            private void drain() {
                try {
                    for (;;) {
                        StreamingQueryCaptureItem next;
                        boolean complete;
                        synchronized (this) {
                            if (cancelled) {
                                draining = false;
                                return;
                            }
                            complete = sourceComplete && buffered.isEmpty();
                            if (complete) {
                                cancelled = true;
                                draining = false;
                                next = null;
                            } else {
                                if (demand == 0L) {
                                    draining = false;
                                    return;
                                }
                                next = buffered.poll();
                                if (next != null) {
                                    demand--;
                                }
                            }
                        }
                        if (complete) {
                            if (nextOrdinal != committed.itemCount()) {
                                throw new QueryCaptureStoreException(
                                    "Committed streaming Query capture expected " + committed.itemCount()
                                        + " items but replay found " + nextOrdinal);
                            }
                            subscriber.onComplete();
                            return;
                        }
                        if (next != null) {
                            subscriber.onNext(next);
                            continue;
                        }
                        loadPage();
                    }
                } catch (Throwable failure) {
                    synchronized (this) {
                        cancelled = true;
                        draining = false;
                    }
                    subscriber.onError(failure instanceof QueryCaptureStoreException
                        ? failure
                        : new QueryCaptureStoreException("Failed replaying streaming Query capture", failure));
                }
            }

            private void loadPage() {
                QueryRequest.Builder request = QueryRequest.builder()
                    .tableName(tableName)
                    .keyConditionExpression("#captureKey = :captureKey AND #revision <= :commitRevision")
                    .expressionAttributeNames(Map.of(
                        "#captureKey", CAPTURE_KEY,
                        "#revision", REVISION))
                    .expressionAttributeValues(Map.of(
                        ":captureKey", stringValue(captureKey),
                        ":commitRevision", numberValue(commitRevision)))
                    .consistentRead(true)
                    .scanIndexForward(true)
                    .limit(REPLAY_PAGE_SIZE);
                if (!cursor.isEmpty()) {
                    request.exclusiveStartKey(cursor);
                }
                QueryResponse page = client.query(request.build());
                List<StreamingQueryCaptureItem> decoded = new ArrayList<>();
                for (Map<String, AttributeValue> row : page.items()) {
                    int schema = Math.toIntExact(longValue(row, SCHEMA_VERSION));
                    if (schema != QueryCaptureEventCodec.SCHEMA_VERSION) {
                        throw new QueryCaptureStoreException(
                            "Unsupported streaming Query capture item schema version " + schema);
                    }
                    QueryCaptureEventCodec.Event event = codec.decode(stringValue(row, EVENT_JSON));
                    if (event.kind() == QueryCaptureEventCodec.Kind.STREAM_ITEM
                        && event.generation() == committed.generation()) {
                        verifyReplayIdentity(committed, event);
                        StreamingQueryCaptureItem item = codec.toItem(event);
                        if (item.ordinal() != nextOrdinal + decoded.size()) {
                            throw new QueryCaptureStoreException(
                                "Streaming Query capture has a corrupt ordinal sequence");
                        }
                        decoded.add(item);
                    }
                }
                synchronized (this) {
                    buffered.addAll(decoded);
                    nextOrdinal += decoded.size();
                    cursor = page.lastEvaluatedKey();
                    sourceComplete = cursor == null || cursor.isEmpty();
                }
            }
        }
    }

    private static void verifyReplayIdentity(
        QueryCaptureEventCodec.Event committed,
        QueryCaptureEventCodec.Event item
    ) {
        if (!committed.captureKey().equals(item.captureKey())
            || !committed.tenantId().equals(item.tenantId())
            || !committed.executionId().equals(item.executionId())
            || committed.stepIndex() != item.stepIndex()
            || !committed.queryId().equals(item.queryId())
            || !committed.queryVersion().equals(item.queryVersion())
            || !committed.inputDigest().equals(item.inputDigest())
            || !committed.outputType().equals(item.outputType())) {
            throw new QueryCaptureStoreException(
                "Streaming Query capture item does not match its committed observation identity");
        }
    }

    @FunctionalInterface
    private interface ThrowingAction {
        void run() throws Exception;
    }

    private sealed interface OpenDecision {
        record Ready(StreamingQueryCaptureOpen open) implements OpenDecision {
        }

        enum Wait implements OpenDecision {
            INSTANCE
        }
    }

    private record StoredEvent(long revision, QueryCaptureEventCodec.Event event) {
    }
}
