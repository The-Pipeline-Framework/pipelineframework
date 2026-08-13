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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ApmCompatibilityMetricsTest {

    @Test
    void recordsTransactionMetrics() {
        InMemoryMetricReader metricReader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(metricReader)
            .build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
            .setMeterProvider(meterProvider)
            .build();
        GlobalOpenTelemetry.resetForTest();
        GlobalOpenTelemetry.set(sdk);

        try {
            ApmCompatibilityMetrics.recordOrchestratorSuccess(123.0);
            ApmCompatibilityMetrics.recordOrchestratorFailure(456.0);

            Collection<MetricData> metrics = metricReader.collectAllMetrics();
            MetricData count = metrics.stream()
                .filter(metric -> "apm.service.transaction.count".equals(metric.getName()))
                .findFirst()
                .orElseThrow();
            MetricData duration = metrics.stream()
                .filter(metric -> "apm.service.transaction.duration".equals(metric.getName()))
                .findFirst()
                .orElseThrow();
            boolean hasTransactionName = count.getLongSumData().getPoints().stream()
                .anyMatch(point -> "OtherTransaction/OrchestratorService/Run".equals(
                    point.getAttributes().get(AttributeKey.stringKey("transaction.name"))));
            assertTrue(hasTransactionName);
            assertTrue(duration.getHistogramData().getPoints().stream()
                .anyMatch(point -> "OtherTransaction/OrchestratorService/Run".equals(
                    point.getAttributes().get(AttributeKey.stringKey("transaction.name")))));
        } finally {
            meterProvider.shutdown();
            GlobalOpenTelemetry.resetForTest();
        }
    }
}
