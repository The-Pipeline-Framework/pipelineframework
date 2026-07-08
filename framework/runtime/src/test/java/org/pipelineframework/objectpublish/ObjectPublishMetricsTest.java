package org.pipelineframework.objectpublish;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.boundary.PipelineObjectNamingConfig;
import org.pipelineframework.config.boundary.PipelineObjectOutputConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishGroupingConfig;
import org.pipelineframework.config.boundary.PipelineOutputBoundaryConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.telemetry.ObjectPublishReplayTelemetry;

class ObjectPublishMetricsTest {

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
    void publishItemsRecordsLowCardinalityObjectPublishMetrics() {
        RecordingProvider provider = new RecordingProvider();
        ObjectPublishRunner runner = new ObjectPublishRunner(
            config(),
            new ObjectTargetRegistry(List.of(provider)),
            new ObjectPublishReplayTelemetry());

        runner.publishItems(List.of(new TestOutput("a", "one"))).await().indefinitely();
        runner.publishItems(List.of()).await().indefinitely();

        var metrics = metricReader.collectAllMetrics();
        assertTrue(hasMetric(metrics, "tpf.object_publish.grouped.total"));
        assertTrue(hasMetric(metrics, "tpf.object_publish.published.total"));
        assertTrue(hasMetric(metrics, "tpf.object_publish.published.bytes.total"));
        assertTrue(hasMetric(metrics, "tpf.object_publish.skipped.total"));
        assertTrue(hasMetric(metrics, "tpf.object_publish.write.duration"));
        assertFalse(hasAttribute(metrics, "key"));
        assertFalse(hasAttribute(metrics, "object_key"));
    }

    @Test
    void publishItemsRecordsObjectPublishFailureMetrics() {
        ObjectPublishRunner runner = new ObjectPublishRunner(
            config(),
            new ObjectTargetRegistry(List.of(new FailingProvider())),
            new ObjectPublishReplayTelemetry());

        assertThrows(RuntimeException.class, () ->
            runner.publishItems(List.of(new TestOutput("a", "one"))).await().indefinitely());

        assertTrue(hasMetric(metricReader.collectAllMetrics(), "tpf.object_publish.failed.total"));
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

    private static PipelineYamlConfig config() {
        PipelineObjectPublishConfig target = new PipelineObjectPublishConfig(
            "results",
            "object",
            "test",
            Map.of(),
            new PipelineObjectNamingConfig("{groupKey}.out"),
            null,
            new PipelineObjectPublishGroupingConfig(32));
        return new PipelineYamlConfig(
            "org.pipelineframework.objectpublish",
            "GRPC",
            "COMPUTE",
            List.of(),
            Map.of(),
            Map.of(),
            Map.of("results", target),
            List.of(),
            null,
            new PipelineOutputBoundaryConfig(null, new PipelineObjectOutputConfig(
                "results",
                TestOutput.class.getName(),
                "TestOutput",
                TestMapper.class.getName())));
    }

    public static final class TestMapper implements ObjectPublishMapper<TestOutput> {
        @Override
        public String groupKey(TestOutput item) {
            return item.group();
        }

        @Override
        public ObjectPayload render(String groupKey, List<TestOutput> items) {
            String body = String.join("\n", items.stream().map(TestOutput::value).toList());
            return new ObjectPayload(body.getBytes(StandardCharsets.UTF_8), "text/plain", Map.of());
        }
    }

    record TestOutput(String group, String value) {
    }

    private static final class RecordingProvider implements ObjectTargetProvider {
        private final List<ObjectWriteRequest> writes = new ArrayList<>();

        @Override
        public String providerName() {
            return "test";
        }

        @Override
        public CompletionStage<ObjectWriteResult> write(ObjectWriteRequest request) {
            writes.add(request);
            return CompletableFuture.completedFuture(new ObjectWriteResult(null, request.bytes().length, request.checksum(), null));
        }

        @Override
        public CompletionStage<ObjectWriteSession> open(ObjectWriteOpenRequest request) {
            return CompletableFuture.completedFuture(new ObjectWriteSession() {
                @Override
                public CompletionStage<Void> write(ByteBuffer chunk) {
                    return CompletableFuture.completedFuture(null);
                }

                @Override
                public CompletionStage<ObjectWriteResult> close(ObjectWriteCloseRequest closeRequest) {
                    return CompletableFuture.completedFuture(
                        new ObjectWriteResult(null, closeRequest.bytes(), closeRequest.checksum(), null));
                }

                @Override
                public CompletionStage<Void> abort(Throwable cause) {
                    return CompletableFuture.completedFuture(null);
                }
            });
        }
    }

    private static final class FailingProvider implements ObjectTargetProvider {
        @Override
        public String providerName() {
            return "test";
        }

        @Override
        public CompletionStage<ObjectWriteResult> write(ObjectWriteRequest request) {
            CompletableFuture<ObjectWriteResult> failure = new CompletableFuture<>();
            failure.completeExceptionally(new IllegalStateException("publish failed"));
            return failure;
        }

        @Override
        public CompletionStage<ObjectWriteSession> open(ObjectWriteOpenRequest request) {
            return CompletableFuture.failedFuture(new IllegalStateException("open failed"));
        }
    }
}
