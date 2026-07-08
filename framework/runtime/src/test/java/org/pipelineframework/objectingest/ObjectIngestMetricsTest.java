package org.pipelineframework.objectingest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.boundary.PipelineInputBoundaryConfig;
import org.pipelineframework.config.boundary.PipelineObjectInputConfig;
import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;
import org.pipelineframework.telemetry.ObjectIngestReplayTelemetry;

class ObjectIngestMetricsTest {

    private InMemoryMetricReader metricReader;
    private SdkMeterProvider meterProvider;

    @BeforeEach
    void setUp() {
        metricReader = InMemoryMetricReader.create();
        meterProvider = SdkMeterProvider.builder().registerMetricReader(metricReader).build();
        GlobalOpenTelemetry.resetForTest();
        GlobalOpenTelemetry.set(OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build());
    }

    @AfterEach
    void tearDown() {
        meterProvider.close();
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    void pollOnceRecordsLowCardinalityObjectIngestMetrics() {
        PipelineObjectSourceConfig source = new PipelineObjectSourceConfig(
            "documents",
            "object",
            "test",
            Map.of(),
            null,
            null,
            null,
            null);
        PipelineYamlConfig config = new PipelineYamlConfig(
            "org.pipelineframework.objectingest",
            "GRPC",
            "COMPUTE",
            List.of(),
            Map.of("documents", source),
            List.of(),
            new PipelineInputBoundaryConfig(null, new PipelineObjectInputConfig(
                "documents",
                ObjectIngestRunnerTest.TestInput.class.getName(),
                "TestInput",
                ObjectIngestRunnerTest.TestMapper.class.getName())),
            null);
        ObjectIngestRunner runner = new ObjectIngestRunner(
            config,
            new ObjectSourceRegistry(List.of(new TwoItemProvider())),
            (input, tenantId, idempotencyKey) -> Uni.createFrom().item(
                idempotencyKey.endsWith("beta.txt:v1:etag-2")
                    ? new RunAsyncAcceptedDto("execution-2", true, "/executions/execution-2", 1L)
                    : new RunAsyncAcceptedDto("execution-1", false, "/executions/execution-1", 1L)),
            new ObjectIngestReplayTelemetry());

        runner.pollOnce();

        var metrics = metricReader.collectAllMetrics();
        assertTrue(hasMetric(metrics, "tpf.object_ingest.list.total"));
        assertTrue(hasMetric(metrics, "tpf.object_ingest.listed.objects.total"));
        assertTrue(hasMetric(metrics, "tpf.object_ingest.submitted.total"));
        assertTrue(hasMetric(metrics, "tpf.object_ingest.duplicate.total"));
        assertFalse(hasAttribute(metrics, "key"));
        assertFalse(hasAttribute(metrics, "execution_id"));
    }

    @Test
    void pollOnceRecordsObjectIngestFailureMetrics() {
        ObjectIngestRunner runner = new ObjectIngestRunner(
            config(),
            new ObjectSourceRegistry(List.of(new TwoItemProvider())),
            (input, tenantId, idempotencyKey) -> Uni.createFrom().failure(new IllegalStateException("admission failed")),
            new ObjectIngestReplayTelemetry());

        ObjectIngestRunner.PollResult result = runner.pollOnce();

        assertEquals(2, result.failed());
        assertTrue(hasMetric(metricReader.collectAllMetrics(), "tpf.object_ingest.failed.total"));
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
            if (metric.getType() == io.opentelemetry.sdk.metrics.data.MetricDataType.LONG_SUM
                && metric.getLongSumData().getPoints().stream()
                    .map(point -> point.getAttributes())
                    .anyMatch(attrs -> hasKey(attrs, key))) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasKey(Attributes attributes, String key) {
        return attributes.asMap().keySet().stream().anyMatch(attributeKey -> key.equals(attributeKey.getKey()));
    }

    private static PipelineYamlConfig config() {
        PipelineObjectSourceConfig source = new PipelineObjectSourceConfig(
            "documents",
            "object",
            "test",
            Map.of(),
            null,
            null,
            null,
            null);
        return new PipelineYamlConfig(
            "org.pipelineframework.objectingest",
            "GRPC",
            "COMPUTE",
            List.of(),
            Map.of("documents", source),
            List.of(),
            new PipelineInputBoundaryConfig(null, new PipelineObjectInputConfig(
                "documents",
                ObjectIngestRunnerTest.TestInput.class.getName(),
                "TestInput",
                ObjectIngestRunnerTest.TestMapper.class.getName())),
            null);
    }

    private static final class TwoItemProvider implements ObjectSourceProvider {
        @Override
        public String providerName() {
            return "test";
        }

        @Override
        public List<ObjectSourceItem> list(PipelineObjectSourceConfig source, int limit) {
            return List.of(
                new ObjectSourceItem("test", "bucket", "alpha.txt", "v1", "etag-1", 12L, 100L, "text/plain", Map.of(), null, null),
                new ObjectSourceItem("test", "bucket", "beta.txt", "v1", "etag-2", 12L, 100L, "text/plain", Map.of(), null, null));
        }
    }
}
