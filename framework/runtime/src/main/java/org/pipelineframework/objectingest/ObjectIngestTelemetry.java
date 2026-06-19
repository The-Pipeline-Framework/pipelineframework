package org.pipelineframework.objectingest;

/**
 * Minimal telemetry hook for object ingest runners.
 */
public interface ObjectIngestTelemetry {

    ObjectIngestTelemetry NOOP = new ObjectIngestTelemetry() {
    };

    default void listed(String sourceName, String provider, int count) {
    }

    default void submitted(String sourceName, String provider, String key) {
    }

    default void duplicate(String sourceName, String provider, String key) {
    }

    default void failed(String sourceName, String provider, String key, Throwable failure) {
    }
}
