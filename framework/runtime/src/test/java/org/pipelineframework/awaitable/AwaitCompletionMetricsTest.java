package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AwaitCompletionMetricsTest {

    private InMemoryMetricReader metricReader;
    private SdkMeterProvider meterProvider;

    @BeforeEach
    void setUp() {
        AwaitCompletionMetrics.resetForTest();
        metricReader = InMemoryMetricReader.create();
        meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build();
        GlobalOpenTelemetry.resetForTest();
        GlobalOpenTelemetry.set(OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build());
    }

    @AfterEach
    void tearDown() {
        meterProvider.close();
        GlobalOpenTelemetry.resetForTest();
        AwaitCompletionMetrics.resetForTest();
    }

    @Test
    void recordsAwaitLifecycleMetricsWithoutHighCardinalityIds() {
        AwaitInteractionRecord interaction = new AwaitInteractionRecord(
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
            Map.of(),
            2_000L,
            1_000L,
            1_500L,
            100_000L);
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

        AwaitCompletionMetrics.recordInteractionDispatched(interaction);
        AwaitCompletionMetrics.recordUnitDispatchComplete(unit);
        AwaitCompletionMetrics.recordCompletionAdmitted(interaction);
        AwaitCompletionMetrics.recordItemCompleted(interaction, unit);
        AwaitCompletionMetrics.recordEarlyCompletionHeld(interaction, unit);
        AwaitCompletionMetrics.recordResumeReleased(unit);
        AwaitCompletionMetrics.recordUnitTerminal(interaction, unit);
        AwaitCompletionMetrics.recordDroppedCompletion("kafka", "terminal");

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
