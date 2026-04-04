package org.pipelineframework.config.template;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class CurrentAuthoredSurfacesGuardTest {

    private static final Pattern LEGACY_AUTHORED_PROTO_TYPE = Pattern.compile("(?m)^\\s*protoType:");
    private static final Pattern QUOTED_PROTO_TYPE_LITERAL = Pattern.compile("['\"]protoType:");

    @Test
    void currentAuthoredSurfacesDoNotReintroduceLegacyProtoTypeDeclarations() throws Exception {
        Path repoRoot = findRepoRoot();
        List<String> violations = new ArrayList<>();

        Path docsGuide = requireDirectory(repoRoot.resolve("docs/guide"));
        Path examples = requireDirectory(repoRoot.resolve("examples"));
        Path templateGenerator = requireDirectory(repoRoot.resolve("template-generator-node"));
        Path uiExportSurface = requireFile(repoRoot.resolve("web-ui/src/routes/+page.svelte"));

        scanMarkdown(docsGuide, violations);
        scanYaml(examples, violations);
        scanYaml(templateGenerator, violations);
        scanUiExportSurface(uiExportSurface, violations);

        assertTrue(
            violations.isEmpty(),
            "Current authored surfaces must not contain legacy protoType declarations:\n" + String.join("\n", violations));
    }

    private static void scanMarkdown(Path root, List<String> violations) throws IOException {
        try (Stream<Path> stream = Files.walk(root)) {
            stream.filter(Files::isRegularFile)
                .filter(path -> path.toString().endsWith(".md"))
                .filter(path -> !path.toString().contains("/docs/versions/"))
                .forEach(path -> recordViolations(path, LEGACY_AUTHORED_PROTO_TYPE, violations));
        }
    }

    private static void scanYaml(Path root, List<String> violations) throws IOException {
        try (Stream<Path> stream = Files.walk(root)) {
            stream.filter(Files::isRegularFile)
                .filter(path -> {
                    String fileName = path.getFileName().toString();
                    return fileName.endsWith(".yaml") || fileName.endsWith(".yml");
                })
                .forEach(path -> recordViolations(path, LEGACY_AUTHORED_PROTO_TYPE, violations));
        }
    }

    private static void scanUiExportSurface(Path uiRoute, List<String> violations) {
        recordViolations(uiRoute, QUOTED_PROTO_TYPE_LITERAL, violations);
    }

    private static Path findRepoRoot() {
        Path current = Path.of("").toAbsolutePath().normalize();
        while (current != null) {
            if (Files.exists(current.resolve(".git")) || Files.exists(current.resolve("AGENTS.md"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("Could not locate repository root from " + Path.of("").toAbsolutePath().normalize());
    }

    private static Path requireDirectory(Path path) {
        if (!Files.exists(path) || !Files.isDirectory(path)) {
            fail("Expected directory to exist: " + path);
        }
        return path;
    }

    private static Path requireFile(Path path) {
        if (!Files.exists(path) || !Files.isRegularFile(path)) {
            fail("Expected file to exist: " + path);
        }
        return path;
    }

    private static void recordViolations(Path path, Pattern pattern, List<String> violations) {
        try {
            String content = Files.readString(path);
            if (pattern.matcher(content).find()) {
                violations.add(path.toString());
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to scan " + path, ex);
        }
    }
}
