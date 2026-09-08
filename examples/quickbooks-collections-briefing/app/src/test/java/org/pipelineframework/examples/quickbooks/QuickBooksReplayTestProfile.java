package org.pipelineframework.examples.quickbooks;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

public final class QuickBooksReplayTestProfile implements QuarkusTestProfile {
    static final Path REPLAY_DIRECTORY = createReplayDirectory();

    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of(
            "pipeline.telemetry.enabled", "true",
            "pipeline.telemetry.tracing.enabled", "true",
            "pipeline.telemetry.tracing.per-item", "true",
            "pipeline.telemetry.replay.enabled", "true",
            "pipeline.telemetry.replay.exporter", "file",
            "pipeline.telemetry.replay.file.path", REPLAY_DIRECTORY.toString(),
            "quarkus.otel.traces.exporter", "none");
    }

    private static Path createReplayDirectory() {
        Path directory = Path.of("target", "quickbooks-demo-replay").toAbsolutePath().normalize();
        try {
            return Files.createDirectories(directory);
        } catch (IOException failure) {
            throw new IllegalStateException("cannot create the QuickBooks demo replay directory", failure);
        }
    }
}
