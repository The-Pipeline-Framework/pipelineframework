package org.pipelineframework.query;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;

/**
 * In-memory query capture store for tests, local development, and unmanaged defaults.
 */
@ApplicationScoped
public class InMemoryQueryCaptureStore implements QueryCaptureStore {
    private final ConcurrentMap<String, QueryCaptureRecord> records = new ConcurrentHashMap<>();

    @Override
    public Uni<Optional<QueryCaptureRecord>> get(String captureKey) {
        return Uni.createFrom().item(Optional.ofNullable(records.get(captureKey)));
    }

    @Override
    public Uni<QueryCaptureRecord> putIfAbsent(QueryCaptureRecord record) {
        QueryCaptureRecord existing = records.putIfAbsent(record.captureKey(), record);
        return Uni.createFrom().item(existing == null ? record : existing);
    }

    @Override
    public Uni<Boolean> remove(String captureKey) {
        return Uni.createFrom().item(records.remove(captureKey) != null);
    }

    @Override
    public Uni<Void> clear() {
        records.clear();
        return Uni.createFrom().voidItem();
    }
}
