package org.pipelineframework.telemetry;

import java.util.List;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import org.pipelineframework.telemetry.derivation.QueryObservationDerivation;

/** Trace-only adapter for live and replayed Query observation metadata. */
final class QueryObservationTracing {
    private static final AttributeKey<Boolean> REPLAYED = AttributeKey.booleanKey("tpf.query.replayed");
    private static final AttributeKey<Long> INPUT_TOKENS = AttributeKey.longKey("gen_ai.usage.input_tokens");
    private static final AttributeKey<Long> OUTPUT_TOKENS = AttributeKey.longKey("gen_ai.usage.output_tokens");
    private static final AttributeKey<Long> TOTAL_TOKENS = AttributeKey.longKey("tpf.query.usage.total_tokens");
    private static final AttributeKey<String> RESPONSE_MODEL = AttributeKey.stringKey("gen_ai.response.model");
    private static final AttributeKey<List<String>> FINISH_REASONS =
        AttributeKey.stringArrayKey("gen_ai.response.finish_reasons");
    private static final AttributeKey<String> PROVIDER = AttributeKey.stringKey("tpf.connector.provider");
    private static final AttributeKey<String> OPERATION = AttributeKey.stringKey("tpf.connector.operation");

    private final Tracer tracer;

    QueryObservationTracing(TelemetryRuntime runtime) {
        tracer = runtime.tracer("org.pipelineframework.query");
    }

    void record(QueryObservationDerivation.Signal signal) {
        Span span = tracer.spanBuilder("tpf.query.observation")
            .setSpanKind(SpanKind.INTERNAL)
            .startSpan();
        try {
            span.setAttribute(REPLAYED, signal.replayed());
            span.setAttribute(PROVIDER, signal.provider());
            span.setAttribute(OPERATION, signal.operation());
            signal.inputTokens().ifPresent(value -> span.setAttribute(INPUT_TOKENS, value));
            signal.outputTokens().ifPresent(value -> span.setAttribute(OUTPUT_TOKENS, value));
            signal.totalTokens().ifPresent(value -> span.setAttribute(TOTAL_TOKENS, value));
            signal.responseModel().ifPresent(value -> span.setAttribute(RESPONSE_MODEL, value));
            signal.finishReason().ifPresent(value -> span.setAttribute(FINISH_REASONS, List.of(value)));
        } finally {
            span.end();
        }
    }
}
