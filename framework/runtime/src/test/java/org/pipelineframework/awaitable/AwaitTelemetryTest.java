package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.telemetry.TelemetryPolicy;
import org.pipelineframework.telemetry.TelemetryRuntime;
import org.pipelineframework.telemetry.RetryAmplificationGuardMode;
import java.time.Duration;

class AwaitTelemetryTest {

    private InMemoryMetricReader metricReader;
    private SdkMeterProvider meterProvider;
    private SdkTracerProvider tracerProvider;
    private InMemorySpanExporter spanExporter;
    private AwaitTelemetry awaitTelemetry;

    @BeforeEach
    void setUp() {
        metricReader = InMemoryMetricReader.create();
        meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build();
        spanExporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build();
        TelemetryRuntime runtime = new TelemetryRuntime() {
            @Override public io.opentelemetry.api.metrics.Meter meter(String scope) { return meterProvider.get(scope); }
            @Override public io.opentelemetry.api.trace.Tracer tracer(String scope) { return tracerProvider.get(scope); }
            @Override public void flush() { }
        };
        awaitTelemetry = new AwaitTelemetry(new TelemetryPolicy(true, true, true, false, true, false,
            Duration.ofSeconds(30), 10d, 3, RetryAmplificationGuardMode.FAIL_FAST), runtime);
    }

    @AfterEach
    void tearDown() {
        meterProvider.close();
        tracerProvider.close();
    }

    @Test
    void recordsAwaitLifecycleMetricsWithoutHighCardinalityIds() {
        AwaitInteractionRecord interaction = interactionRecord();
        AwaitUnitRecord unit = new AwaitUnitRecord(
            "tenant-1",
            "unit-1",
            "exec-1",
            "Await Payment Provider",
            1,
            "ONE_TO_ONE",
            1L,
            AwaitUnitStatus.COMPLETED,
            null,
            1,
            1,
            Set.of("item:0"),
            true,
            1_000L,
            1_750L,
            100_000L);

        awaitTelemetry.recordInteractionDispatched(interaction);
        awaitTelemetry.recordUnitDispatchComplete(unit);
        awaitTelemetry.recordCompletionAdmitted(interaction);
        awaitTelemetry.recordItemCompleted(interaction, unit);
        awaitTelemetry.recordEarlyCompletionHeld(interaction, unit);
        awaitTelemetry.recordResumeReleased(unit);
        awaitTelemetry.recordUnitTerminal(interaction, unit);
        awaitTelemetry.recordDroppedCompletion("kafka", "terminal");

        var metrics = metricReader.collectAllMetrics();
        assertTrue(hasMetric(metrics, "tpf.await.interaction.dispatched.total"));
        assertTrue(hasMetric(metrics, "tpf.await.unit.dispatch_complete.total"));
        assertTrue(hasMetric(metrics, "tpf.await.completion.admitted.total"));
        assertTrue(hasMetric(metrics, "tpf.await.item.completed.total"));
        assertTrue(hasMetric(metrics, "tpf.await.completion.early_held.total"));
        assertTrue(hasMetric(metrics, "tpf.await.resume.released.total"));
        assertTrue(hasMetric(metrics, "tpf.await.unit.terminal.total"));
        assertTrue(hasMetric(metrics, "tpf.await.completion.latency"));
        assertTrue(hasMetric(metrics, "tpf.await.unit.duration"));
        assertTrue(hasMetric(metrics, "tpf.await.completion.dropped.total"));
        assertFalse(hasAttribute(metrics, "tpf.await.unit_id"));
        assertFalse(hasAttribute(metrics, "tpf.await.interaction_id"));
        assertFalse(hasAttribute(metrics, "tpf.await.execution_id"));
    }

    @Test
    void keepsProviderDispatchSpanCurrentForTheActualSubscription() {
        AtomicBoolean spanWasCurrent = new AtomicBoolean();

        String value = awaitTelemetry.inProviderDispatchSpan(interactionRecord(),
            () -> Uni.createFrom().item(() -> {
                spanWasCurrent.set(io.opentelemetry.api.trace.Span.current().getSpanContext().isValid());
                return "dispatched";
            }))
            .await().indefinitely();

        assertTrue(spanWasCurrent.get());
        assertTrue("dispatched".equals(value));
    }

    @Test
    void completionAddsDurableLinkToCapturedOrigin() {
        Span origin = tracerProvider.get("await-test").spanBuilder("origin").startSpan();
        Map<String, Object> traceMetadata;
        try (Scope ignored = origin.makeCurrent()) {
            traceMetadata = awaitTelemetry.captureTraceMetadata();
        } finally {
            origin.end();
        }

        awaitTelemetry.recordCompletionAdmitted(interactionRecord(traceMetadata));

        var completion = spanExporter.getFinishedSpanItems().stream()
            .filter(span -> "tpf.await.completion.admitted".equals(span.getName()))
            .findFirst()
            .orElseThrow();
        assertTrue(completion.getLinks().stream()
            .anyMatch(link -> origin.getSpanContext().getTraceId().equals(link.getSpanContext().getTraceId())
                && origin.getSpanContext().getSpanId().equals(link.getSpanContext().getSpanId())));
        assertTrue(Boolean.TRUE.equals(completion.getAttributes()
            .get(io.opentelemetry.api.common.AttributeKey.booleanKey("tpf.await.origin.linked"))));
    }

    private static AwaitInteractionRecord interactionRecord() {
        return interactionRecord(Map.of());
    }

    private static AwaitInteractionRecord interactionRecord(Map<String, Object> transportMetadata) {
        return new AwaitInteractionRecord(
            "tenant-1",
            "exec-1",
            "Await Payment Provider",
            1,
            "PaymentStatus",
            "interaction-1",
            "correlation-1",
            "cause-1",
            "idem-1",
            1L,
            AwaitInteractionStatus.COMPLETED,
            "request",
            "response",
            "unit-1",
            0,
            null,
            null,
            null,
            "kafka",
            transportMetadata,
            2_000L,
            1_000L,
            1_500L,
            100_000L);
    }

    private static boolean hasMetric(Iterable<MetricData> metrics, String name) {
        for (MetricData metric : metrics) {
            if (name.equals(metric.getName())) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasAttribute(Iterable<MetricData> metrics, String key) {
        for (MetricData metric : metrics) {
            switch (metric.getType()) {
                case LONG_SUM -> {
                    if (metric.getLongSumData().getPoints().stream()
                        .map(point -> point.getAttributes())
                        .anyMatch(attrs -> hasKey(attrs, key))) {
                        return true;
                    }
                }
                case HISTOGRAM -> {
                    if (metric.getHistogramData().getPoints().stream()
                        .map(point -> point.getAttributes())
                        .anyMatch(attrs -> hasKey(attrs, key))) {
                        return true;
                    }
                }
                default -> {
                }
            }
        }
        return false;
    }

    private static boolean hasKey(Attributes attributes, String key) {
        return attributes.asMap().keySet().stream().anyMatch(attributeKey -> key.equals(attributeKey.getKey()));
    }
}
