package org.pipelineframework.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.QueryObservation;
import org.pipelineframework.connector.QueryTokenUsage;

class QueryObservationTelemetryTest {
    private InMemoryMetricReader metricReader;
    private InMemorySpanExporter spanExporter;
    private SdkMeterProvider meterProvider;
    private SdkTracerProvider tracerProvider;
    private QueryObservationTelemetry telemetry;

    @BeforeEach
    void setUp() {
        metricReader = InMemoryMetricReader.create();
        spanExporter = InMemorySpanExporter.create();
        meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build();
        tracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
            .setMeterProvider(meterProvider)
            .setTracerProvider(tracerProvider)
            .build();
        telemetry = new QueryObservationTelemetry(new TelemetryRuntime() {
            @Override
            public Meter meter(String instrumentationScope) {
                return sdk.getMeter(instrumentationScope);
            }

            @Override
            public Tracer tracer(String instrumentationScope) {
                return sdk.getTracer(instrumentationScope);
            }

            @Override
            public void flush() {
            }
        });
    }

    @AfterEach
    void tearDown() {
        tracerProvider.shutdown();
        meterProvider.shutdown();
    }

    @Test
    void recordsLiveInputAndOutputOnceAndKeepsTotalOnTheSpan() {
        telemetry.record(operation(), observation());

        var metric = metricReader.collectAllMetrics().stream()
            .filter(candidate -> "gen_ai.client.token.usage".equals(candidate.getName()))
            .findFirst().orElseThrow();
        var points = metric.getHistogramData().getPoints();

        assertEquals("{token}", metric.getUnit());
        assertEquals(2, points.size());
        assertEquals(Set.of(12d, 4d), points.stream()
            .map(point -> point.getSum()).collect(Collectors.toSet()));
        assertEquals(Set.of("input", "output"), points.stream()
            .map(point -> point.getAttributes().get(AttributeKey.stringKey("gen_ai.token.type")))
            .collect(Collectors.toSet()));
        points.forEach(point -> {
            assertEquals("tpf.llm.openai", point.getAttributes().get(
                AttributeKey.stringKey("tpf.connector.provider")));
            assertEquals("turn", point.getAttributes().get(
                AttributeKey.stringKey("tpf.connector.operation")));
        });

        var span = spanExporter.getFinishedSpanItems().getFirst();
        assertEquals("tpf.query.observation", span.getName());
        assertEquals(false, span.getAttributes().get(AttributeKey.booleanKey("tpf.query.replayed")));
        assertEquals(12L, span.getAttributes().get(AttributeKey.longKey("gen_ai.usage.input_tokens")));
        assertEquals(4L, span.getAttributes().get(AttributeKey.longKey("gen_ai.usage.output_tokens")));
        assertEquals(40L, span.getAttributes().get(AttributeKey.longKey("tpf.query.usage.total_tokens")));
        assertEquals("provider-model", span.getAttributes().get(AttributeKey.stringKey("gen_ai.response.model")));
        assertEquals(List.of("stop"), span.getAttributes().get(
            AttributeKey.stringArrayKey("gen_ai.response.finish_reasons")));
    }

    @Test
    void replayAndAbsentUsageEmitNoNewUsagePointsAndUseOnlyAllowListedSpanAttributes() {
        telemetry.record(operation(), observation().asReplay());
        telemetry.record(operation(), QueryObservation.live(
            Optional.empty(), Optional.empty(), Optional.empty()));

        assertTrue(metricReader.collectAllMetrics().stream()
            .noneMatch(candidate -> "gen_ai.client.token.usage".equals(candidate.getName())));
        assertEquals(2, spanExporter.getFinishedSpanItems().size());
        var replay = spanExporter.getFinishedSpanItems().getFirst();
        assertEquals(true, replay.getAttributes().get(AttributeKey.booleanKey("tpf.query.replayed")));

        Set<String> allowed = Set.of(
            "tpf.query.replayed",
            "gen_ai.usage.input_tokens",
            "gen_ai.usage.output_tokens",
            "tpf.query.usage.total_tokens",
            "gen_ai.response.model",
            "gen_ai.response.finish_reasons",
            "tpf.connector.provider",
            "tpf.connector.operation");
        spanExporter.getFinishedSpanItems().forEach(span -> {
            Set<String> keys = span.getAttributes().asMap().keySet().stream()
                .map(AttributeKey::getKey).collect(Collectors.toSet());
            assertTrue(allowed.containsAll(keys), keys.toString());
            assertFalse(span.getAttributes().toString().contains("secret prompt"));
            assertFalse(span.getAttributes().toString().contains("api-key"));
        });
    }

    private static ConnectorOperationIdentity operation() {
        return new ConnectorOperationIdentity(
            ConnectorProviderId.of("tpf.llm.openai"), "turn", ConnectorOperationKind.QUERY, 1);
    }

    private static QueryObservation observation() {
        return QueryObservation.live(
            Optional.of(new QueryTokenUsage(
                OptionalLong.of(12), OptionalLong.of(4), OptionalLong.of(40))),
            Optional.of("provider-model"), Optional.of("stop"));
    }
}
