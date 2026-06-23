package org.pipelineframework.objectpublish;

import java.time.Duration;

/**
 * Telemetry hook for Object Publish lifecycle events.
 */
public interface ObjectPublishTelemetry {
    ObjectPublishTelemetry NOOP = new ObjectPublishTelemetry() {
    };

    default void grouped(String targetName, int itemCount, int groupCount) {
    }

    default void published(String targetName, String provider, String objectKey, long bytes) {
    }

    default void skipped(String targetName) {
    }

    default void failed(String targetName, String provider, String objectKey, Throwable failure) {
    }

    default void writeDuration(String targetName, String provider, Duration duration) {
    }
}
