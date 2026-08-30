package org.pipelineframework.blocking;

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/** Demand-aware blocking iterator publisher and the lifetime of its owned resources. */
public record BlockingIteratorPublisher<T>(Flow.Publisher<T> rows, CompletionStage<Void> termination) {
    public BlockingIteratorPublisher {
        rows = Objects.requireNonNull(rows, "blocking iterator rows must not be null");
        termination = Objects.requireNonNull(termination, "blocking iterator termination must not be null");
    }
}
