package org.pipelineframework.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import io.smallrye.mutiny.Multi;

class InMemoryQueryCaptureStoreTest {

    private static final Instant FIXED_TIME = Instant.parse("2024-01-01T00:00:00Z");

    private QueryCaptureRecord record(String tenantId, String executionId, int stepIndex,
            String queryId, String captureKey) {
        return new QueryCaptureRecord(
            tenantId, executionId, stepIndex,
            queryId, "v1", captureKey,
            "{\"id\":\"123\"}", "{\"riskScore\":42}",
            "com.example.RiskSnapshot",
            FIXED_TIME);
    }

    @Test
    void returnsEmptyOptionalForUnknownCaptureKey() {
        InMemoryQueryCaptureStore store = new InMemoryQueryCaptureStore();

        Optional<QueryCaptureRecord> result = store.get("unknown-key").toCompletableFuture().join();

        assertFalse(result.isPresent());
    }

    @Test
    void storesAndRetrievesRecordByCaptureKey() {
        InMemoryQueryCaptureStore store = new InMemoryQueryCaptureStore();
        QueryCaptureRecord record = record("tenant-1", "exec-abc", 2,
            "customer-risk-by-id", "tenant-1:exec-abc:2:cust-123");

        store.putIfAbsent(record).toCompletableFuture().join();
        Optional<QueryCaptureRecord> result = store.get("tenant-1:exec-abc:2:cust-123").toCompletableFuture().join();

        assertTrue(result.isPresent());
        assertEquals(record, result.get());
    }

    @Test
    void putIfAbsentReturnsStoredRecordOnFirstPut() {
        InMemoryQueryCaptureStore store = new InMemoryQueryCaptureStore();
        QueryCaptureRecord record = record("tenant-1", "exec-abc", 2,
            "query-id", "key-1");

        QueryCaptureRecord returned = store.putIfAbsent(record).toCompletableFuture().join();

        assertEquals(record, returned);
    }

    @Test
    void putIfAbsentReturnsExistingRecordOnDuplicateKey() {
        InMemoryQueryCaptureStore store = new InMemoryQueryCaptureStore();
        QueryCaptureRecord first = record("tenant-1", "exec-abc", 2,
            "query-id", "key-1");
        QueryCaptureRecord second = new QueryCaptureRecord(
            "tenant-1", "exec-abc", 2,
            "query-id", "v1", "key-1",
            "{\"id\":\"456\"}", "{\"riskScore\":99}",
            "com.example.RiskSnapshot",
            FIXED_TIME);

        store.putIfAbsent(first).toCompletableFuture().join();
        QueryCaptureRecord result = store.putIfAbsent(second).toCompletableFuture().join();

        assertEquals(first, result);
        assertEquals("{\"riskScore\":42}", result.outputJson());
    }

    @Test
    void storesMultipleDistinctCaptureKeys() {
        InMemoryQueryCaptureStore store = new InMemoryQueryCaptureStore();
        QueryCaptureRecord record1 = record("tenant-1", "exec-abc", 2,
            "query-id", "key-1");
        QueryCaptureRecord record2 = record("tenant-1", "exec-abc", 3,
            "query-id", "key-2");

        store.putIfAbsent(record1).toCompletableFuture().join();
        store.putIfAbsent(record2).toCompletableFuture().join();

        Optional<QueryCaptureRecord> result1 = store.get("key-1").toCompletableFuture().join();
        Optional<QueryCaptureRecord> result2 = store.get("key-2").toCompletableFuture().join();

        assertTrue(result1.isPresent());
        assertTrue(result2.isPresent());
        assertEquals(record1, result1.get());
        assertEquals(record2, result2.get());
    }

    @Test
    void providerNameReturnsMemory() {
        InMemoryQueryCaptureStore store = new InMemoryQueryCaptureStore();

        assertEquals("memory", store.providerName());
    }

    @Test
    void getReturnsNonNullCompletionStage() {
        InMemoryQueryCaptureStore store = new InMemoryQueryCaptureStore();

        assertNotNull(store.get("any-key"));
    }

    @Test
    void getReturnsPresentForKeyStoredInDifferentExecution() {
        InMemoryQueryCaptureStore store = new InMemoryQueryCaptureStore();
        QueryCaptureRecord first = record("tenant-1", "exec-001", 1, "q", "shared-key");
        QueryCaptureRecord second = record("tenant-1", "exec-002", 1, "q", "shared-key");

        store.putIfAbsent(first).toCompletableFuture().join();
        QueryCaptureRecord stored = store.putIfAbsent(second).toCompletableFuture().join();

        // putIfAbsent should return the first stored record
        assertEquals(first.executionId(), stored.executionId());
    }

    @Test
    void streamingCaptureAbortsPartialItemsThenCommitsAndReplaysOnlyTheSuccessfulAttempt() throws Exception {
        InMemoryQueryCaptureStore store = new InMemoryQueryCaptureStore();
        StreamingQueryCaptureRequest request = streamingRequest("stream-key");
        StreamingQueryCaptureWriter first = ((StreamingQueryCaptureOpen.Write) store.openStreaming(request)
            .toCompletableFuture().join()).writer();
        first.append(streamingItem(0, "A")).toCompletableFuture().join();
        var waiting = store.openStreaming(request).toCompletableFuture();

        first.abort().toCompletableFuture().join();
        StreamingQueryCaptureWriter retry = ((StreamingQueryCaptureOpen.Write) waiting.get(
            2, java.util.concurrent.TimeUnit.SECONDS)).writer();
        retry.append(streamingItem(0, "A")).toCompletableFuture().join();
        retry.append(streamingItem(1, "B")).toCompletableFuture().join();
        retry.commit().toCompletableFuture().join();

        StreamingQueryCaptureOpen.Replay replay = (StreamingQueryCaptureOpen.Replay) store.openStreaming(request)
            .toCompletableFuture().join();
        List<StreamingQueryCaptureItem> items = Multi.createFrom().publisher(replay.items())
            .collect().asList().await().atMost(Duration.ofSeconds(2));
        assertEquals(List.of(streamingItem(0, "A"), streamingItem(1, "B")), items);
    }

    @Test
    void streamingCaptureCommitsAndReplaysAnEmptyObservation() {
        InMemoryQueryCaptureStore store = new InMemoryQueryCaptureStore();
        StreamingQueryCaptureRequest request = streamingRequest("empty-stream-key");
        StreamingQueryCaptureWriter writer = ((StreamingQueryCaptureOpen.Write) store.openStreaming(request)
            .toCompletableFuture().join()).writer();

        writer.commit().toCompletableFuture().join();

        StreamingQueryCaptureOpen.Replay replay = (StreamingQueryCaptureOpen.Replay) store.openStreaming(request)
            .toCompletableFuture().join();
        assertEquals(List.of(), Multi.createFrom().publisher(replay.items())
            .collect().asList().await().atMost(Duration.ofSeconds(2)));
    }

    @Test
    void abortedCaptureReopensEachWaiterWithItsOwnObservationRequest() throws Exception {
        InMemoryQueryCaptureStore store = new InMemoryQueryCaptureStore();
        StreamingQueryCaptureRequest firstRequest = streamingRequest("shared-key");
        StreamingQueryCaptureRequest waitingRequest = new StreamingQueryCaptureRequest(
            "tenant", "execution", 2, "find.many", "v1", "shared-key", "{}", Integer.class.getName());
        StreamingQueryCaptureWriter first = ((StreamingQueryCaptureOpen.Write) store.openStreaming(firstRequest)
            .toCompletableFuture().join()).writer();
        var waiting = store.openStreaming(waitingRequest).toCompletableFuture();

        first.abort().toCompletableFuture().join();
        StreamingQueryCaptureWriter reopened = ((StreamingQueryCaptureOpen.Write) waiting.get(
            2, java.util.concurrent.TimeUnit.SECONDS)).writer();

        reopened.append(new StreamingQueryCaptureItem(0, "1", Integer.class.getName()))
            .toCompletableFuture().join();
    }

    @Test
    void clearClosesActiveWritersAndReleasesWaitersExceptionally() {
        InMemoryQueryCaptureStore store = new InMemoryQueryCaptureStore();
        StreamingQueryCaptureRequest request = streamingRequest("clear-key");
        StreamingQueryCaptureWriter writer = ((StreamingQueryCaptureOpen.Write) store.openStreaming(request)
            .toCompletableFuture().join()).writer();
        var waiting = store.openStreaming(request).toCompletableFuture();

        store.clear().toCompletableFuture().join();

        assertThrows(java.util.concurrent.CompletionException.class, waiting::join);
        assertThrows(java.util.concurrent.CompletionException.class,
            () -> writer.append(streamingItem(0, "A")).toCompletableFuture().join());
        assertTrue(store.openStreaming(request).toCompletableFuture().join()
            instanceof StreamingQueryCaptureOpen.Write);
    }

    private static StreamingQueryCaptureRequest streamingRequest(String key) {
        return new StreamingQueryCaptureRequest(
            "tenant", "execution", 2, "find.many", "v1", key, "{}", String.class.getName());
    }

    private static StreamingQueryCaptureItem streamingItem(long ordinal, String value) {
        return new StreamingQueryCaptureItem(ordinal, "\"" + value + "\"", String.class.getName());
    }
}
