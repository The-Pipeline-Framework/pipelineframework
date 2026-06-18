package com.example.smoke;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpringSmokeDependencyGuardTest {
    private static final List<String> PROHIBITED_SOURCE_TOKENS = List.of(
        "io.quarkus.",
        "jakarta.enterprise.",
        "io.vertx.",
        "org.jboss.resteasy.",
        "jakarta.ws.rs.");

    @Test
    void springSmokeDoesNotExposeQuarkusRuntimeApi() {
        assertThrows(
            ClassNotFoundException.class,
            () -> Class.forName("org.pipelineframework.service.ReactiveService"));
    }

    @Test
    void generatedSpringSourcesDoNotContainQuarkusImports() throws IOException {
        Path generatedSourcesDir = Path.of(
            System.getProperty("project.build.directory", "target"),
            "generated-sources",
            "pipeline");
        assertTrue(Files.isDirectory(generatedSourcesDir), "Expected generated sources in " + generatedSourcesDir);

        try (var paths = Files.walk(generatedSourcesDir)) {
            List<Path> javaSources = paths
                .filter(path -> path.toString().endsWith(".java"))
                .toList();
            assertFalse(javaSources.isEmpty(), "Expected generated Java sources under " + generatedSourcesDir);

            for (Path source : javaSources) {
                String content = Files.readString(source);
                for (String prohibitedToken : PROHIBITED_SOURCE_TOKENS) {
                    assertFalse(
                        content.contains(prohibitedToken),
                        () -> "Generated Spring source contains prohibited token '" + prohibitedToken + "': " + source);
                }
            }
        }
    }
}
