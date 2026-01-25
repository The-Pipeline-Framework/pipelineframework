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

package org.pipelineframework.rest;

import java.util.Collection;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.service.ReactiveBidirectionalStreamingService;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.service.ReactiveStreamingClientService;
import org.pipelineframework.service.ReactiveStreamingService;
import org.pipelineframework.telemetry.HttpMetrics;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RestAdapterTelemetryTest {

    @Test
    void emitsHttpSloMetricsForAllAdapterShapes() {
        InMemoryMetricReader metricReader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(metricReader)
            .build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
            .setMeterProvider(meterProvider)
            .build();
        GlobalOpenTelemetry.resetForTest();
        GlobalOpenTelemetry.set(sdk);
        HttpMetrics.resetForTest();

        try {
            new UnaryAdapter().remoteProcess("a").await().indefinitely();
            new StreamingAdapter().remoteProcess("a").collect().asList().await().indefinitely();
            new StreamingClientAdapter().remoteProcess(Multi.createFrom().items("a", "b")).await().indefinitely();
            new BidirectionalAdapter().remoteProcess(Multi.createFrom().items("a", "b"))
                .collect().asList().await().indefinitely();
            HttpMetrics.instrumentClient("ProcessPaymentStatusService", "process", Uni.createFrom().item("ok"))
                .await().indefinitely();

            Collection<MetricData> metrics = metricReader.collectAllMetrics();
            assertTrue(hasMetric(metrics, "rpc.server.requests"), "Missing rpc.server.requests metric");
            assertTrue(hasMetric(metrics, "tpf.slo.rpc.server.total"), "Missing tpf.slo.rpc.server.total metric");
            assertTrue(hasHttpSystem(metrics), "Missing rpc.system=http attribute");
            assertTrue(hasMetric(metrics, "tpf.slo.rpc.client.total"), "Missing tpf.slo.rpc.client.total metric");
        } finally {
            meterProvider.shutdown();
            GlobalOpenTelemetry.resetForTest();
        }
    }

    private boolean hasMetric(Collection<MetricData> metrics, String name) {
        return metrics.stream().anyMatch(metric -> name.equals(metric.getName()));
    }

    private boolean hasHttpSystem(Collection<MetricData> metrics) {
        AttributeKey<String> rpcSystem = AttributeKey.stringKey("rpc.system");
        return metrics.stream()
            .filter(metric -> "rpc.server.requests".equals(metric.getName()))
            .flatMap(metric -> metric.getLongSumData().getPoints().stream())
            .anyMatch(point -> "http".equals(point.getAttributes().get(rpcSystem)));
    }

    private static final class UnaryAdapter extends RestReactiveServiceAdapter<String, String, String, String> {
        private final ReactiveService<String, String> service = new UnaryService();

        @Override
        protected ReactiveService<String, String> getService() {
            return service;
        }

        @Override
        protected String fromDto(String dtoIn) {
            return dtoIn;
        }

        @Override
        protected String toDto(String domainOut) {
            return domainOut;
        }
    }

    private static final class StreamingAdapter
        extends RestReactiveStreamingServiceAdapter<String, String, String, String> {
        private final ReactiveStreamingService<String, String> service = new StreamingService();

        @Override
        protected ReactiveStreamingService<String, String> getService() {
            return service;
        }

        @Override
        protected String fromDto(String dtoIn) {
            return dtoIn;
        }

        @Override
        protected String toDto(String domainOut) {
            return domainOut;
        }
    }

    private static final class StreamingClientAdapter
        extends RestReactiveStreamingClientServiceAdapter<String, String, String, String> {
        private final ReactiveStreamingClientService<String, String> service = new StreamingClientService();

        @Override
        protected ReactiveStreamingClientService<String, String> getService() {
            return service;
        }

        @Override
        protected String fromDto(String dtoIn) {
            return dtoIn;
        }

        @Override
        protected String toDto(String domainOut) {
            return domainOut;
        }
    }

    private static final class BidirectionalAdapter
        extends RestReactiveBidirectionalStreamingServiceAdapter<String, String, String, String> {
        private final ReactiveBidirectionalStreamingService<String, String> service = new BidirectionalService();

        @Override
        protected ReactiveBidirectionalStreamingService<String, String> getService() {
            return service;
        }

        @Override
        protected String fromDto(String dtoIn) {
            return dtoIn;
        }

        @Override
        protected String toDto(String domainOut) {
            return domainOut;
        }
    }

    private static final class UnaryService implements ReactiveService<String, String> {
        @Override
        public Uni<String> process(String input) {
            return Uni.createFrom().item(input.toUpperCase());
        }
    }

    private static final class StreamingService implements ReactiveStreamingService<String, String> {
        @Override
        public Multi<String> process(String input) {
            return Multi.createFrom().items(input, input + "-2");
        }
    }

    private static final class StreamingClientService implements ReactiveStreamingClientService<String, String> {
        @Override
        public Uni<String> process(Multi<String> input) {
            return input.collect().asList().map(values -> String.join(",", values));
        }
    }

    private static final class BidirectionalService implements ReactiveBidirectionalStreamingService<String, String> {
        @Override
        public Multi<String> process(Multi<String> input) {
            return input.map(value -> value + "-out");
        }
    }
}
