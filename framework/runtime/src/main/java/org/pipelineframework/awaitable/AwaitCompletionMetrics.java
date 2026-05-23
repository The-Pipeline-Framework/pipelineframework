package org.pipelineframework.awaitable;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;

/**
 * Await completion observability helpers.
 */
public final class AwaitCompletionMetrics {

    private static final AttributeKey<String> TRANSPORT = AttributeKey.stringKey("tpf.await.transport");
    private static final AttributeKey<String> REASON = AttributeKey.stringKey("tpf.await.completion.reason");

    private static volatile LongCounter droppedCompletionCounter;

    private AwaitCompletionMetrics() {
    }

    public static void recordDroppedCompletion(String transport, String reason) {
        ensureInitialized();
        droppedCompletionCounter.add(1, Attributes.builder()
            .put(TRANSPORT, normalize(transport))
            .put(REASON, normalize(reason))
            .build());
    }

    private static void ensureInitialized() {
        if (droppedCompletionCounter != null) {
            return;
        }
        synchronized (AwaitCompletionMetrics.class) {
            if (droppedCompletionCounter != null) {
                return;
            }
            droppedCompletionCounter = GlobalOpenTelemetry.getMeter("org.pipelineframework")
                .counterBuilder("tpf.await.completion.dropped.total")
                .setDescription("Total deterministic await completions dropped by transport consumers")
                .setUnit("events")
                .build();
        }
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        return value;
    }
}
