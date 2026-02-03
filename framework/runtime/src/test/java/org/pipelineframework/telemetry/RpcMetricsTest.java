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

import java.util.Collection;

import io.grpc.Status;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RpcMetricsTest {

    @Test
    void recordsGrpcServerMetrics() {
        InMemoryMetricReader metricReader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(metricReader)
            .build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
            .setMeterProvider(meterProvider)
            .build();
        GlobalOpenTelemetry.resetForTest();
        GlobalOpenTelemetry.set(sdk);
        RpcMetrics.resetForTest();

        try {
            RpcMetrics.recordGrpcServer("ProcessPaymentStatusService", "remoteProcess", Status.Code.OK, 1_000_000);

            Collection<MetricData> metrics = metricReader.collectAllMetrics();
            MetricData requests = metrics.stream()
                .filter(metric -> "rpc.server.requests".equals(metric.getName()))
                .findFirst()
                .orElseThrow();
            boolean hasSloTotals = metrics.stream()
                .anyMatch(metric -> "tpf.slo.rpc.server.total".equals(metric.getName()));
            boolean hasSloLatency = metrics.stream()
                .anyMatch(metric -> "tpf.slo.rpc.server.latency.total".equals(metric.getName()));

            boolean hasService = requests.getLongSumData().getPoints().stream()
                .anyMatch(point -> "ProcessPaymentStatusService".equals(
                    point.getAttributes().get(AttributeKey.stringKey("rpc.service"))));
            assertTrue(hasService);
            assertTrue(hasSloTotals);
            assertTrue(hasSloLatency);
        } finally {
            meterProvider.shutdown();
            GlobalOpenTelemetry.resetForTest();
        }
    }
}
