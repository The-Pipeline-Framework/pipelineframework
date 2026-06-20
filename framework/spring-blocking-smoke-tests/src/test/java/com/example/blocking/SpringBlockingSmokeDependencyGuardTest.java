package com.example.blocking;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpringBlockingSmokeDependencyGuardTest {
    private static final List<String> PROHIBITED_SOURCE_TOKENS = List.of(
        "io.quarkus.",
        "jakarta.enterprise.",
        "io.vertx.",
        "io.smallrye.mutiny",
        "org.jboss.resteasy.",
        "jakarta.ws.rs.");

    @Test
    void springBlockingSmokeDoesNotExposeQuarkusRuntimeApi() {
        assertThrows(
            ClassNotFoundException.class,
            () -> Class.forName("org.pipelineframework.service.ReactiveService"));
    }

    @Test
    void springBlockingSmokePomDoesNotDeclareDirectMutinyDependency() throws IOException {
        Path pom = Path.of(System.getProperty("user.dir"), "pom.xml");
        String content = Files.readString(pom);
        assertFalse(content.contains("<artifactId>mutiny</artifactId>"), "Spring blocking smoke module must not declare Mutiny directly");
    }

    @Test
    void generatedSpringSourcesDoNotContainQuarkusImports() throws IOException {
        Path generatedSourcesDir = generatedSourcesDir();
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

    @Test
    void generatedBlockingLocalStepDoesNotUseReactiveLibraries() throws IOException {
        Path generatedStep = findGeneratedSource("ProcessPaymentLocalClientStep.java");
        String content = Files.readString(generatedStep);

        assertTrue(content.contains("RuntimeAdapters.executeBlocking"));
        assertTrue(content.contains("this.paymentService.processBlocking(input)"));
        assertTrue(content.contains(", true)"));
        assertFalse(content.contains("io.smallrye.mutiny"));
        assertFalse(content.contains("reactor.core.publisher"));
    }

    private Path generatedSourcesDir() {
        return Path.of(
            System.getProperty("project.build.directory", "target"),
            "generated-sources",
            "pipeline");
    }

    private Path findGeneratedSource(String fileName) throws IOException {
        Path generatedSourcesDir = generatedSourcesDir();
        try (var paths = Files.walk(generatedSourcesDir)) {
            return paths
                .filter(path -> path.getFileName().toString().equals(fileName))
                .findFirst()
                .orElseThrow(() -> new IOException("Generated source not found: " + fileName));
        }
    }
}
