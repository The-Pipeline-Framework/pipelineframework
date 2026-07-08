package org.pipelineframework.orchestrator.controlplane;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class ControlPlaneSemanticApiTest {

    private static final Pattern MUTABLE_MARK_VERB = Pattern.compile("\\bmark[A-Z][A-Za-z0-9_]*\\b");

    @Test
    void immutableSemanticPackageDoesNotExposeMarkStyleOperations() throws Exception {
        Path sourceDir = Path.of("src/main/java/org/pipelineframework/orchestrator/controlplane");
        try (var files = Files.walk(sourceDir)) {
            String offenders = files
                .filter(path -> path.toString().endsWith(".java"))
                .filter(path -> !path.getFileName().toString().equals("ControlPlaneSemanticApiTest.java"))
                .filter(this::containsMarkVerb)
                .map(Path::toString)
                .sorted()
                .reduce("", (left, right) -> left.isBlank() ? right : left + "\n" + right);
            assertTrue(offenders.isBlank(), "new immutable control-plane model must not expose mark* verbs:\n" + offenders);
        }
    }

    private boolean containsMarkVerb(Path path) {
        try {
            return MUTABLE_MARK_VERB.matcher(Files.readString(path)).find();
        } catch (Exception e) {
            throw new IllegalStateException("Failed reading " + path, e);
        }
    }
}
