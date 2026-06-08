package org.pipelineframework;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RuntimeAdapterSeamTest {

    @Test
    void runtimeUsesVertxOnlyInAdapterBootstrap() {
        Path sourceRoot = Path.of("src/main/java");
        Path allowedSeam = sourceRoot.resolve("org/pipelineframework/runtime/RuntimeAdapterBootstrap.java").normalize();

        List<String> violations = new ArrayList<>();
        try (var stream = Files.walk(sourceRoot)) {
            stream.filter(path -> path.toString().endsWith(".java")).forEach(path -> {
                if (path.normalize().equals(allowedSeam)) {
                    return;
                }

                try {
                    List<String> lines = Files.readAllLines(path);
                    for (int lineNo = 1; lineNo <= lines.size(); lineNo++) {
                        if (lines.get(lineNo - 1).contains("io.vertx.")) {
                            violations.add(path + ":" + lineNo);
                        }
                    }
                } catch (IOException ignored) {
                    // Ignore transient read issues while scanning source files.
                }
            });
        } catch (IOException e) {
            violations.add("Unable to scan runtime sources: " + e.getMessage());
        }

        assertTrue(violations.isEmpty(), "Vert.x usage must stay in runtime adapter seam: " + violations);
    }
}
