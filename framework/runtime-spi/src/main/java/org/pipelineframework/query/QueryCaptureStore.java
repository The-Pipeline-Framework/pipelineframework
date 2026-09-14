package org.pipelineframework.query;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;

/**
 * Store used by captured query steps to replay prior read results for an execution.
 */
public interface QueryCaptureStore {
    default String providerName() {
        return "memory";
    }

    CompletionStage<Optional<QueryCaptureRecord>> get(String captureKey);

    CompletionStage<QueryCaptureRecord> putIfAbsent(QueryCaptureRecord record);

    default CompletionStage<StreamingQueryCaptureOpen> openStreaming(
        StreamingQueryCaptureRequest request
    ) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
            "Query capture store '" + providerName() + "' does not support streaming Query capture"));
    }

    CompletionStage<Boolean> remove(String captureKey);

    CompletionStage<Void> clear();
}
