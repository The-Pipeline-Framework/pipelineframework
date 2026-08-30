package org.pipelineframework.telemetry;

import java.util.Objects;

import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.QueryObservation;
import org.pipelineframework.telemetry.derivation.QueryObservationDerivation;

/** Coordinates independent metric and tracing adapters at the Query invocation/capture boundary. */
public final class QueryObservationTelemetry {
    private final QueryObservationMetrics metrics;
    private final QueryObservationTracing tracing;

    public QueryObservationTelemetry(TelemetryRuntime runtime) {
        Objects.requireNonNull(runtime, "telemetry runtime must not be null");
        metrics = new QueryObservationMetrics(runtime);
        tracing = new QueryObservationTracing(runtime);
    }

    public static QueryObservationTelemetry global() {
        return new QueryObservationTelemetry(
            TelemetryCompatibilityAccess.metricsRuntime(),
            TelemetryCompatibilityAccess.tracingRuntime());
    }

    private QueryObservationTelemetry(TelemetryRuntime metricsRuntime, TelemetryRuntime tracingRuntime) {
        metrics = new QueryObservationMetrics(metricsRuntime);
        tracing = new QueryObservationTracing(tracingRuntime);
    }

    public void record(ConnectorOperationIdentity operation, QueryObservation observation) {
        QueryObservationDerivation.Signal signal;
        try {
            signal = QueryObservationDerivation.derive(operation, observation);
        } catch (RuntimeException ignored) {
            return;
        }
        try {
            metrics.record(signal);
        } catch (RuntimeException ignored) {
            // Metric export must never change the application result or suppress tracing.
        }
        try {
            tracing.record(signal);
        } catch (RuntimeException ignored) {
            // Observation telemetry must never change the application result.
        }
    }
}
