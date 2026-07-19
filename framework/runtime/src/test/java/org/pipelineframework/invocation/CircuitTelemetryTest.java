package org.pipelineframework.invocation;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.jupiter.api.Test;
import org.pipelineframework.runtime.core.resilience.CircuitIdentity;
import org.pipelineframework.runtime.core.resilience.CircuitOpen;
import org.pipelineframework.runtime.core.resilience.CircuitPolicy;
import org.pipelineframework.runtime.core.resilience.CircuitScope;
import org.pipelineframework.runtime.core.resilience.CircuitStateTransition;

class CircuitTelemetryTest {

    @Test
    void recordsAdmissionAndTransitionAttributes() {
        InMemoryMetricReader reader = InMemoryMetricReader.create();
        try (SdkMeterProvider meterProvider = SdkMeterProvider.builder().registerMetricReader(reader).build()) {
            Meter meter = meterProvider.get("circuit-telemetry-test");
            CircuitTelemetry telemetry = new CircuitTelemetry(meter);
            TransportBoundaryDescriptor boundary = new TransportBoundaryDescriptor("grpc", "pricing.remoteProcess");
            CircuitIdentity identity = new CircuitIdentity("pricing-service");
            CircuitPolicy policy = new CircuitPolicy(
                CircuitScope.LOCAL_PROCESS,
                5,
                Duration.ofMinutes(1),
                Duration.ofSeconds(30),
                1,
                Duration.ofSeconds(1));

            telemetry.permitted(boundary, new ResolvedCircuitPolicy(identity, policy));
            telemetry.rejected(boundary, new CircuitOpen(
                identity, CircuitScope.LOCAL_PROCESS, Instant.now().plusSeconds(30)));
            telemetry.onTransition(
                identity, CircuitScope.LOCAL_PROCESS, CircuitStateTransition.CLOSED_TO_OPEN);

            Collection<MetricData> metrics = reader.collectAllMetrics();
            assertTrue(hasPoint(metrics, "tpf.circuit.admissions", Attributes.builder()
                .put("tpf.circuit.identity", "pricing-service")
                .put("tpf.circuit.scope", "LOCAL_PROCESS")
                .put("tpf.transport.protocol", "grpc")
                .put("tpf.transport.target", "pricing.remoteProcess")
                .put("tpf.circuit.admission", "permitted")
                .build()));
            assertTrue(hasPoint(metrics, "tpf.circuit.admissions", Attributes.builder()
                .put("tpf.circuit.identity", "pricing-service")
                .put("tpf.circuit.scope", "LOCAL_PROCESS")
                .put("tpf.transport.protocol", "grpc")
                .put("tpf.transport.target", "pricing.remoteProcess")
                .put("tpf.circuit.admission", "rejected")
                .build()));
            assertTrue(hasPoint(metrics, "tpf.circuit.transitions", Attributes.builder()
                .put("tpf.circuit.identity", "pricing-service")
                .put("tpf.circuit.scope", "LOCAL_PROCESS")
                .put("tpf.circuit.transition", "CLOSED_TO_OPEN")
                .build()));
        }
    }

    private static boolean hasPoint(Collection<MetricData> metrics, String name, Attributes attributes) {
        return metrics.stream()
            .filter(metric -> name.equals(metric.getName()))
            .flatMap(metric -> metric.getLongSumData().getPoints().stream())
            .anyMatch(point -> point.getValue() == 1 && point.getAttributes().equals(attributes));
    }
}
