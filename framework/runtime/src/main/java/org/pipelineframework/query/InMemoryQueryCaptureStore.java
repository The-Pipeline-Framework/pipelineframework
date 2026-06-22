package org.pipelineframework.query;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * In-memory query capture store for tests, local development, and unmanaged defaults.
 */
@ApplicationScoped
public class InMemoryQueryCaptureStore implements QueryCaptureStore {
    private final ConcurrentMap<String, QueryCaptureRecord> records = new ConcurrentHashMap<>();

    @Override
    public CompletionStage<Optional<QueryCaptureRecord>> get(String captureKey) {
        return CompletableFuture.completedFuture(Optional.ofNullable(records.get(captureKey)));
    }

    @Override
    public CompletionStage<QueryCaptureRecord> putIfAbsent(QueryCaptureRecord record) {
        QueryCaptureRecord existing = records.putIfAbsent(record.captureKey(), record);
        return CompletableFuture.completedFuture(existing == null ? record : existing);
    }

    @Override
    public CompletionStage<Boolean> remove(String captureKey) {
        return CompletableFuture.completedFuture(records.remove(captureKey) != null);
    }

    @Override
    public CompletionStage<Void> clear() {
        records.clear();
        return CompletableFuture.completedFuture(null);
    }
}
