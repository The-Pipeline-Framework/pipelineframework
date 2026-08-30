package org.pipelineframework.telemetry;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import org.pipelineframework.telemetry.derivation.QueryObservationDerivation;

/** Metric-only adapter for newly consumed provider token usage. */
final class QueryObservationMetrics {
    private static final AttributeKey<String> TOKEN_TYPE = AttributeKey.stringKey("gen_ai.token.type");
    private static final AttributeKey<String> PROVIDER = AttributeKey.stringKey("tpf.connector.provider");
    private static final AttributeKey<String> OPERATION = AttributeKey.stringKey("tpf.connector.operation");

    private final LongHistogram tokenUsage;

    QueryObservationMetrics(TelemetryRuntime runtime) {
        tokenUsage = runtime.meter("org.pipelineframework.query")
            .histogramBuilder("gen_ai.client.token.usage")
            .setDescription("Provider-reported tokens consumed by live Query observations")
            .setUnit("{token}")
            .ofLongs()
            .build();
    }

    void record(QueryObservationDerivation.Signal signal) {
        if (signal.replayed()) {
            return;
        }
        signal.inputTokens().ifPresent(value -> tokenUsage.record(value, attributes(signal, "input")));
        signal.outputTokens().ifPresent(value -> tokenUsage.record(value, attributes(signal, "output")));
    }

    private static Attributes attributes(QueryObservationDerivation.Signal signal, String tokenType) {
        return Attributes.builder()
            .put(TOKEN_TYPE, tokenType)
            .put(PROVIDER, signal.provider())
            .put(OPERATION, signal.operation())
            .build();
    }
}
