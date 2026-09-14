package org.pipelineframework.runtime.spi;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuntimeSpiDependencyGuardTest {

    @Test
    void runtimeSpiHasNoQuarkusCdiSpringOrSerializationImports() {
        assertNoForbiddenDependency("io.quarkus.");
        assertNoForbiddenDependency("jakarta.enterprise.");
        assertNoForbiddenDependency("jakarta.inject.");
        assertNoForbiddenDependency("org.springframework.");
        assertNoForbiddenDependency("com.fasterxml.jackson.");
        assertNoForbiddenDependency("com.google.protobuf.");
    }

    @Test
    void runtimeSpiHasNoRuntimeImplementationImports() {
        assertNoForbiddenDependency("org.pipelineframework.runtime.");
    }

    @Test
    void forbiddenDependencyMatchingCoversRegularAndStaticImports() {
        assertTrue(isForbiddenImport("import com.google.protobuf.ByteString;", "com.google.protobuf."));
        assertTrue(isForbiddenImport(
            "import static com.google.protobuf.ByteString.copyFrom;", "com.google.protobuf."));
        assertFalse(isForbiddenImport(
            "return \"com.google.protobuf.ByteString\";", "com.google.protobuf."));
    }

    private void assertNoForbiddenDependency(String forbiddenToken) {
        List<String> violations = collectViolations(forbiddenToken);
        assertTrue(
            violations.isEmpty(),
            "runtime-spi has forbidden dependency token '" + forbiddenToken + "':\n" + String.join("\n", violations));
    }

    private List<String> collectViolations(String token) {
        try {
            Path sourceRoot = Path.of("src/main/java");
            if (!Files.isDirectory(sourceRoot)) {
                return List.of("Unable to scan runtime-spi sources: missing directory " + sourceRoot);
            }

            Map<Path, List<Integer>> matches = new HashMap<>();
            List<String> scanErrors = new ArrayList<>();
            try (var stream = Files.walk(sourceRoot)) {
                stream.filter(path -> path.toString().endsWith(".java")).forEach(path -> {
                    try {
                        List<String> lines = Files.readAllLines(path);
                        for (int lineNo = 1; lineNo <= lines.size(); lineNo++) {
                            if (isForbiddenImport(lines.get(lineNo - 1), token)) {
                                matches.computeIfAbsent(path, ignored -> new ArrayList<>()).add(lineNo);
                            }
                        }
                    } catch (IOException e) {
                        scanErrors.add("Unable to read " + path + ": " + e.getMessage());
                    }
                });
            }

            List<String> violations = new ArrayList<>();
            matches.forEach((path, lineNumbers) -> {
                for (Integer lineNo : lineNumbers) {
                    violations.add(path + ":" + lineNo);
                }
            });
            violations.addAll(scanErrors);
            Collections.sort(violations);
            return violations;
        } catch (IOException e) {
            return List.of("Unable to scan runtime-spi sources: " + e.getMessage());
        }
    }

    private boolean isForbiddenImport(String line, String token) {
        String declaration = line.stripLeading();
        return declaration.startsWith("import ") && declaration.contains(token);
    }
}
