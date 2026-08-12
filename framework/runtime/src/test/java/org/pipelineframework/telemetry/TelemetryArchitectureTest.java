package org.pipelineframework.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class TelemetryArchitectureTest {
    @Test
    void onlyProductionTelemetryRuntimeAccessesTheJvmGlobalSdk() throws Exception {
        Path sourceRoot = Path.of("src", "main", "java", "org", "pipelineframework");
        try (var paths = Files.walk(sourceRoot)) {
            List<Path> globalUsers = paths.filter(path -> path.toString().endsWith(".java"))
                .filter(path -> contains(path, "GlobalOpenTelemetry"))
                .toList();
            assertEquals(List.of(sourceRoot.resolve("telemetry/GlobalTelemetryRuntime.java")), globalUsers);
        }
    }

    @Test
    void pureObservationAndAttributeCoreDoesNotUseSdkOrCdi() throws Exception {
        for (String name : List.of("TelemetryObservation.java", "TelemetryAttributes.java", "TelemetryPolicy.java")) {
            String source = Files.readString(Path.of("src", "main", "java", "org", "pipelineframework", "telemetry", name));
            assertFalse(source.contains("io.opentelemetry"));
            assertFalse(source.contains("jakarta."));
        }
    }

    private static boolean contains(Path path, String value) {
        try {
            return Files.readString(path).contains(value);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }
}
