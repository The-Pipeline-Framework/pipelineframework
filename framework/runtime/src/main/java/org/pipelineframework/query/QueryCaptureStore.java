package org.pipelineframework.query;

import java.util.Optional;

import io.smallrye.mutiny.Uni;

/**
 * Store used by captured query steps to replay prior read results for an execution.
 */
public interface QueryCaptureStore {
    default String providerName() {
        return "memory";
    }

    Uni<Optional<QueryCaptureRecord>> get(String captureKey);

    Uni<QueryCaptureRecord> putIfAbsent(QueryCaptureRecord record);
}
