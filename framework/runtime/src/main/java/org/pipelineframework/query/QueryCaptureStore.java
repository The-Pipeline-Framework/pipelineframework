package org.pipelineframework.query;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Store used by captured query steps to replay prior read results for an execution.
 */
public interface QueryCaptureStore {
    default String providerName() {
        return "memory";
    }

    CompletionStage<Optional<QueryCaptureRecord>> get(String captureKey);

    CompletionStage<QueryCaptureRecord> putIfAbsent(QueryCaptureRecord record);

    CompletionStage<Boolean> remove(String captureKey);

    CompletionStage<Void> clear();
}
