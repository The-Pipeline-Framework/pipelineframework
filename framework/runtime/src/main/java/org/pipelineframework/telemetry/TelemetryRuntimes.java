package org.pipelineframework.telemetry;

/** Compatibility access point for legacy static emitters while they are migrated to injection. */
public final class TelemetryRuntimes {
    private static final TelemetryRuntime GLOBAL = new GlobalTelemetryRuntime();

    private TelemetryRuntimes() {
    }

    public static TelemetryRuntime global() {
        return GLOBAL;
    }
}
