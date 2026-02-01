/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.telemetry;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.smallrye.mutiny.Multi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.telemetry.RetryAmplificationGuardMode;

import static org.junit.jupiter.api.Assertions.*;

class PipelineTelemetryTest {

    private InMemorySpanExporter exporter;
    private SdkTracerProvider tracerProvider;
    private InMemoryMetricReader metricReader;
    private SdkMeterProvider meterProvider;

    @BeforeEach
    void setUp() {
        exporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .build();
        metricReader = InMemoryMetricReader.create();
        meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(metricReader)
            .build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setMeterProvider(meterProvider)
            .build();
        GlobalOpenTelemetry.resetForTest();
        GlobalOpenTelemetry.set(sdk);
    }

    @AfterEach
    void tearDown() {
        tracerProvider.shutdown();
        meterProvider.shutdown();
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    void recordsRunAttributesForParallelismAndBackpressure() {
        PipelineTelemetry telemetry = new PipelineTelemetry(new TestPipelineStepConfig());
        Multi<Integer> input = Multi.createFrom().items(1, 2, 3);
        PipelineTelemetry.RunContext runContext =
            telemetry.startRun(input, 1, ParallelismPolicy.AUTO, 4);

        Multi<Integer> instrumented = (Multi<Integer>) telemetry.instrumentInput(input, runContext);
        Multi<Integer> stepped =
            telemetry.instrumentStepMulti(DummyStep.class, instrumented, runContext, true);
        Multi<Integer> completed =
            (Multi<Integer>) telemetry.instrumentRunCompletion(stepped, runContext);

        completed.collect().asList().await().indefinitely();

        List<SpanData> spans = exporter.getFinishedSpanItems();
        SpanData runSpan = spans.stream()
            .filter(span -> "tpf.pipeline.run".equals(span.getName()))
            .findFirst()
            .orElseThrow();
        Attributes attributes = runSpan.getAttributes();

        assertNotNull(attributes.get(AttributeKey.longKey("tpf.parallel.max_in_flight")));
        assertNotNull(attributes.get(AttributeKey.doubleKey("tpf.parallel.avg_in_flight")));

        assertTrue(attributes.get(AttributeKey.longKey("tpf.parallel.max_in_flight")) >= 1L);
    }

    @Test
    void exposesStepInflightGauge() {
        PipelineTelemetry telemetry = new PipelineTelemetry(new TestPipelineStepConfig());
        Multi<Integer> input = Multi.createFrom().items(1, 2);
        PipelineTelemetry.RunContext runContext =
            telemetry.startRun(input, 1, ParallelismPolicy.AUTO, 4);

        Multi<Integer> instrumented = (Multi<Integer>) telemetry.instrumentInput(input, runContext);
        Multi<Integer> stepped =
            telemetry.instrumentStepMulti(DummyStep.class, instrumented, runContext, true);
        Multi<Integer> completed =
            (Multi<Integer>) telemetry.instrumentRunCompletion(stepped, runContext);

        completed.collect().asList().await().indefinitely();

        Collection<MetricData> metrics = metricReader.collectAllMetrics();
        MetricData inflight = metrics.stream()
            .filter(metric -> "tpf.step.inflight".equals(metric.getName()))
            .findFirst()
            .orElseThrow();
        MetricData maxConcurrency = metrics.stream()
            .filter(metric -> "tpf.pipeline.max_concurrency".equals(metric.getName()))
            .findFirst()
            .orElseThrow();

        String stepClass = DummyStep.class.getName();
        assertEquals(1, inflight.getLongGaugeData().getPoints().stream()
            .filter(point -> stepClass.equals(point.getAttributes()
                .get(AttributeKey.stringKey("tpf.step.class"))))
            .count());
        long maxValue = maxConcurrency.getLongGaugeData().getPoints().stream()
            .findFirst()
            .orElseThrow()
            .getValue();
        assertEquals(4L, maxValue);
    }

    @Test
    void resolvesConsumerAndProducerStepForProxyClasses() {
        PipelineTelemetry telemetry = new PipelineTelemetry(new TestPipelineStepConfig());
        PipelineTelemetry.RunContext runContext =
            telemetry.startRun(Multi.createFrom().items(1, 2), 1, ParallelismPolicy.AUTO, 4);

        Multi<Integer> consumed = telemetry.instrumentItemConsumed(
            DummyStep$$Proxy.class, runContext, Multi.createFrom().items(1, 2));
        consumed.collect().asList().await().indefinitely();

        Multi<Integer> produced = telemetry.instrumentItemProduced(
            DummyStep_ClientProxy.class, runContext, Multi.createFrom().items(3, 4));
        produced.collect().asList().await().indefinitely();

        Collection<MetricData> metrics = metricReader.collectAllMetrics();
        MetricData consumedMetric = metrics.stream()
            .filter(metric -> "tpf.item.consumed".equals(metric.getName()))
            .findFirst()
            .orElseThrow();
        MetricData producedMetric = metrics.stream()
            .filter(metric -> "tpf.item.produced".equals(metric.getName()))
            .findFirst()
            .orElseThrow();

        String stepClass = DummyStep.class.getName();
        AttributeKey<String> stepKey = AttributeKey.stringKey("tpf.step.class");

        assertTrue(consumedMetric.getLongSumData().getPoints().stream()
            .anyMatch(point -> stepClass.equals(point.getAttributes().get(stepKey))),
            "Expected consumed metric for resolved step class");
        assertTrue(producedMetric.getLongSumData().getPoints().stream()
            .anyMatch(point -> stepClass.equals(point.getAttributes().get(stepKey))),
            "Expected produced metric for resolved step class");
    }

    @Test
    void recordsItemSuccessSloFromConsumedAndProducedCounts() {
        PipelineTelemetry telemetry = new PipelineTelemetry(new TestPipelineStepConfig());
        PipelineTelemetry.RunContext runContext =
            telemetry.startRun(Multi.createFrom().items(1, 2, 3), 1, ParallelismPolicy.AUTO, 4);

        Multi<Integer> consumed = telemetry.instrumentItemConsumed(
            DummyStep.class, runContext, Multi.createFrom().items(1, 2, 3));
        consumed.collect().asList().await().indefinitely();

        Multi<Integer> produced = telemetry.instrumentItemProduced(
            DummyStep.class, runContext, Multi.createFrom().items(1, 2));
        produced.collect().asList().await().indefinitely();

        Multi<Integer> completed = (Multi<Integer>) telemetry.instrumentRunCompletion(
            Multi.createFrom().item(1), runContext);
        completed.collect().asList().await().indefinitely();

        Collection<MetricData> metrics = metricReader.collectAllMetrics();
        MetricData total = metrics.stream()
            .filter(metric -> "tpf.slo.item.success.total".equals(metric.getName()))
            .findFirst()
            .orElseThrow();
        MetricData good = metrics.stream()
            .filter(metric -> "tpf.slo.item.success.good".equals(metric.getName()))
            .findFirst()
            .orElseThrow();

        long totalValue = total.getLongSumData().getPoints().stream()
            .findFirst()
            .orElseThrow()
            .getValue();
        long goodValue = good.getLongSumData().getPoints().stream()
            .findFirst()
            .orElseThrow()
            .getValue();

        assertEquals(3L, totalValue, "Expected total items to match consumed count");
        assertEquals(2L, goodValue, "Expected good items to match produced count");
    }

    @Test
    void normalizesStepAttributesForProxyClasses() {
        PipelineTelemetry telemetry = new PipelineTelemetry(new TestPipelineStepConfig());
        Multi<Integer> input = Multi.createFrom().items(1, 2);
        PipelineTelemetry.RunContext runContext =
            telemetry.startRun(input, 1, ParallelismPolicy.AUTO, 4);

        Multi<Integer> instrumented = (Multi<Integer>) telemetry.instrumentInput(input, runContext);
        Multi<Integer> stepped =
            telemetry.instrumentStepMulti(DummyStep$$Proxy.class, instrumented, runContext, true);
        Multi<Integer> completed =
            (Multi<Integer>) telemetry.instrumentRunCompletion(stepped, runContext);

        completed.collect().asList().await().indefinitely();

        Collection<MetricData> metrics = metricReader.collectAllMetrics();
        MetricData stepDurationMetric = metrics.stream()
            .filter(metric -> "tpf.step.duration".equals(metric.getName()))
            .findFirst()
            .orElseThrow();

        String resolved = DummyStep.class.getName();
        AttributeKey<String> stepKey = AttributeKey.stringKey("tpf.step.class");
        assertTrue(stepDurationMetric.getHistogramData().getPoints().stream()
            .anyMatch(point -> resolved.equals(point.getAttributes().get(stepKey))),
            "Expected step duration metric to use normalized step class");
    }

    static class DummyStep {
    }

    @Test
    void recordsStepRetryCounter() {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(exporter))
            .build();
        InMemoryMetricReader metricReader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(metricReader)
            .build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setMeterProvider(meterProvider)
            .build();
        GlobalOpenTelemetry.resetForTest();
        GlobalOpenTelemetry.set(sdk);

        try {
            PipelineTelemetry telemetry = new PipelineTelemetry(new TestPipelineStepConfig());
            PipelineTelemetry.recordRetry(DummyStep.class);
            PipelineTelemetry.recordRetry(DummyStep.class);

            Collection<MetricData> metrics = metricReader.collectAllMetrics();
            MetricData retryMetric = metrics.stream()
                .filter(metric -> "tpf.step.retry.count".equals(metric.getName()))
                .findFirst()
                .orElseThrow();

            String stepClass = DummyStep.class.getName();
            long value = retryMetric.getLongSumData().getPoints().stream()
                .filter(point -> stepClass.equals(point.getAttributes()
                    .get(AttributeKey.stringKey("tpf.step.class"))))
                .findFirst()
                .orElseThrow()
                .getValue();
            assertEquals(2L, value);
        } finally {
            tracerProvider.shutdown();
            meterProvider.shutdown();
            GlobalOpenTelemetry.resetForTest();
        }
    }

    static final class DummyStep$$Proxy extends DummyStep {
    }

    static final class DummyStep_ClientProxy extends DummyStep {
    }

    static final class TestPipelineStepConfig implements PipelineStepConfig {
        private final StepConfig defaults = new TestStepConfig();
        private final TelemetryConfig telemetry = new TestTelemetryConfig();
        private final KillSwitchConfig killSwitch = new TestKillSwitchConfig();

        @Override
        public StepConfig defaults() {
            return defaults;
        }

        @Override
        public ParallelismPolicy parallelism() {
            return ParallelismPolicy.AUTO;
        }

        @Override
        public Integer maxConcurrency() {
            return 4;
        }

        @Override
        public HealthConfig health() {
            return () -> Duration.ofMinutes(5);
        }

        @Override
        public CacheConfig cache() {
            return new CacheConfig() {
                @Override
                public Optional<String> provider() {
                    return Optional.empty();
                }

                @Override
                public String policy() {
                    return "prefer-cache";
                }

                @Override
                public Optional<Duration> ttl() {
                    return Optional.empty();
                }

                @Override
                public CaffeineConfig caffeine() {
                    return null;
                }

                @Override
                public RedisConfig redis() {
                    return null;
                }
            };
        }

        @Override
        public TelemetryConfig telemetry() {
            return telemetry;
        }

        @Override
        public KillSwitchConfig killSwitch() {
            return killSwitch;
        }

        @Override
        public Map<String, StepConfig> step() {
            return Map.of();
        }

        @Override
        public Map<String, ModuleConfig> module() {
            return Map.of();
        }

        @Override
        public ClientConfig client() {
            return new ClientConfig() {
                @Override
                public Optional<Integer> basePort() {
                    return Optional.empty();
                }

                @Override
                public Optional<String> tlsConfigurationName() {
                    return Optional.empty();
                }
            };
        }
    }

    static final class TestTelemetryConfig implements PipelineStepConfig.TelemetryConfig {
        @Override
        public Boolean enabled() {
            return true;
        }

        @Override
        public Optional<String> itemInputType() {
            return Optional.empty();
        }

        @Override
        public Optional<String> itemOutputType() {
            return Optional.empty();
        }

        @Override
        public PipelineStepConfig.TracingConfig tracing() {
            return new TestTracingConfig();
        }

        @Override
        public PipelineStepConfig.MetricsConfig metrics() {
            return () -> true;
        }
    }

    static final class TestKillSwitchConfig implements PipelineStepConfig.KillSwitchConfig {
        @Override
        public PipelineStepConfig.RetryAmplificationGuardConfig retryAmplification() {
            return new TestRetryAmplificationGuardConfig();
        }
    }

    static final class TestRetryAmplificationGuardConfig
        implements PipelineStepConfig.RetryAmplificationGuardConfig {
        @Override
        public Boolean enabled() {
            return false;
        }

        @Override
        public Duration window() {
            return Duration.ofSeconds(30);
        }

        @Override
        public Double inflightSlopeThreshold() {
            return 10d;
        }

        @Override
        public Double retryRateThreshold() {
            return 5d;
        }

        @Override
        public RetryAmplificationGuardMode mode() {
            return RetryAmplificationGuardMode.FAIL_FAST;
        }
    }

    static final class TestTracingConfig implements PipelineStepConfig.TracingConfig {
        @Override
        public Boolean enabled() {
            return true;
        }

        @Override
        public Boolean perItem() {
            return false;
        }

        @Override
        public Boolean clientSpansForce() {
            return false;
        }

        @Override
        public Optional<String> clientSpansAllowlist() {
            return Optional.empty();
        }
    }

    static final class TestStepConfig implements PipelineStepConfig.StepConfig {
        @Override
        public Integer retryLimit() {
            return 3;
        }

        @Override
        public Long retryWaitMs() {
            return 2000L;
        }

        @Override
        public Boolean recoverOnFailure() {
            return false;
        }

        @Override
        public Long maxBackoff() {
            return 30000L;
        }

        @Override
        public Boolean jitter() {
            return false;
        }

        @Override
        public Integer backpressureBufferCapacity() {
            return 128;
        }

        @Override
        public String backpressureStrategy() {
            return "BUFFER";
        }
    }

    // Cache settings are unused by these tests.
}
