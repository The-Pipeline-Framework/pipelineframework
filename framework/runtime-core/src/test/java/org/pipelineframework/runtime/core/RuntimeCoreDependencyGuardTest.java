package org.pipelineframework.runtime.core;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RuntimeCoreDependencyGuardTest {

    @Test
    void runtimeCoreHasNoQuarkusDependencies() {
        assertNoForbiddenDependency("io.quarkus.");
    }

    @Test
    void runtimeCoreHasNoVertxDependencies() {
        assertNoForbiddenDependency("io.vertx.");
    }

    private void assertNoForbiddenDependency(String forbiddenToken) {
        List<String> violations = collectViolations(forbiddenToken);
        assertTrue(
            violations.isEmpty(),
            "runtime-core has forbidden dependency token '" + forbiddenToken + "':\n" + String.join("\n", violations));
    }

    private List<String> collectViolations(String token) {
        try {
            Path sourceRoot = Path.of("src/main/java");
            if (!Files.isDirectory(sourceRoot)) {
                return Collections.emptyList();
            }

            Map<Path, List<Integer>> matches = new HashMap<>();
            try (var stream = Files.walk(sourceRoot)) {
                stream.filter(path -> path.toString().endsWith(".java")).forEach(path -> {
                    try {
                        List<String> lines = Files.readAllLines(path);
                        for (int lineNo = 1; lineNo <= lines.size(); lineNo++) {
                            if (lines.get(lineNo - 1).contains(token)) {
                                matches.computeIfAbsent(path, ignored -> new ArrayList<>()).add(lineNo);
                            }
                        }
                    } catch (IOException ignored) {
                        // Best effort: ignore ephemeral file access issues during test execution.
                    }
                });
            }

            List<String> violations = new ArrayList<>();
            matches.forEach((path, lineNumbers) -> {
                for (Integer lineNo : lineNumbers) {
                    violations.add(path + ":" + lineNo);
                }
            });
            Collections.sort(violations);
            return violations;
        } catch (IOException e) {
            return List.of("Unable to scan runtime-core sources: " + e.getMessage());
        }
    }
}
