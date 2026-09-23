package org.pipelineframework.processor;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class CurrentDocumentationAndWebUiGuardTest {

    private static final Pattern LEGACY_PROTO_TYPE_DECLARATION = Pattern.compile("(?m)^\\s*protoType:");
    private static final Pattern QUOTED_PROTO_TYPE_LITERAL = Pattern.compile("['\"]protoType:");

    @Test
    void currentDocumentationAndWebUiDoNotReintroduceLegacyProtoTypeDeclarations() throws IOException {
        Path repoRoot = findRepositoryRoot();
        Path docs = repoRoot.resolve("docs");
        Path webUiRoute = repoRoot.resolve("web-ui/src/routes/+page.svelte");
        List<String> violations = new ArrayList<>();

        try (Stream<Path> files = Files.walk(docs)) {
            files.filter(Files::isRegularFile)
                .filter(path -> path.toString().endsWith(".md"))
                .filter(path -> !path.startsWith(docs.resolve("versions")))
                .forEach(path -> recordViolations(path, LEGACY_PROTO_TYPE_DECLARATION, violations));
        }
        recordViolations(webUiRoute, QUOTED_PROTO_TYPE_LITERAL, violations);

        assertTrue(violations.isEmpty(), "Current docs and web UI contain legacy protoType declarations:\n"
            + String.join("\n", violations));
    }

    private static Path findRepositoryRoot() {
        Path current = Path.of("").toAbsolutePath().normalize();
        while (current != null) {
            if (Files.isRegularFile(current.resolve("AGENTS.md")) && Files.isDirectory(current.resolve("docs"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("Could not locate pipelineframework repository root");
    }

    private static void recordViolations(Path path, Pattern pattern, List<String> violations) {
        try {
            if (pattern.matcher(Files.readString(path)).find()) {
                violations.add(path.toString());
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to scan " + path, ex);
        }
    }
}
