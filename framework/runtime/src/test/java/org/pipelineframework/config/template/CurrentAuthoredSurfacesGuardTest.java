package org.pipelineframework.config.template;

import static org.junit.jupiter.api.Assertions.assertTrue;

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
        Path repoRoot = Path.of("").toAbsolutePath().normalize().resolve("..").resolve("..").normalize();
        List<String> violations = new ArrayList<>();

        scanMarkdown(repoRoot.resolve("docs/guide"), violations);
        scanYaml(repoRoot.resolve("examples"), violations);
        scanYaml(repoRoot.resolve("template-generator-node"), violations);
        scanUiExportSurface(repoRoot.resolve("web-ui/src/routes/+page.svelte"), violations);

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

    private static void scanUiExportSurface(Path uiRoute, List<String> violations) throws IOException {
        recordViolations(uiRoute, QUOTED_PROTO_TYPE_LITERAL, violations);
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
