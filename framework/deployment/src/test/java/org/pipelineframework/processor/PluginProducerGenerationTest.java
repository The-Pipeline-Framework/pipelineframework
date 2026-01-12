package org.pipelineframework.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PluginProducerGenerationTest {

    @TempDir
    Path tempDir;

    @Test
    void generatesSideEffectResourceUsingPluginImplementationClass() throws IOException {
        Path projectRoot = tempDir;
        Files.writeString(projectRoot.resolve("pom.xml"), """
            <project>
              <modelVersion>4.0.0</modelVersion>
              <groupId>com.example</groupId>
              <artifactId>test</artifactId>
              <version>1.0.0</version>
              <packaging>pom</packaging>
            </project>
            """);

        Path moduleDir = projectRoot.resolve("test-module");
        Path generatedSourcesDir = moduleDir.resolve("target/generated-sources/pipeline");
        Files.createDirectories(generatedSourcesDir);

        Files.writeString(projectRoot.resolve("pipeline.yaml"), """
            appName: "Test Pipeline"
            basePackage: "com.example"
            transport: "REST"
            steps:
              - name: "Process Test"
                cardinality: "ONE_TO_ONE"
                inputTypeName: "String"
                outputTypeName: "String"
            aspects:
              persistence:
                enabled: true
                scope: "GLOBAL"
                position: "AFTER_STEP"
                order: 0
                config:
                  pluginImplementationClass: "com.example.PersistenceService"
            """);

        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions("-Apipeline.generatedSourcesDir=" + generatedSourcesDir)
            .compile(JavaFileObjects.forSourceString(
                "com.example.PersistencePluginHost",
                """
                    package com.example;

                    import org.pipelineframework.annotation.PipelinePlugin;

                    @PipelinePlugin("persistence")
                    public class PersistencePluginHost {
                    }
                    """));

        assertThat(compilation).succeeded();

        Path restServerDir = generatedSourcesDir.resolve("rest-server");
        assertTrue(containsText(restServerDir, "PersistenceService"));
    }

    private boolean containsText(Path rootDir, String text) throws IOException {
        if (!Files.exists(rootDir)) {
            return false;
        }
        try (var stream = Files.walk(rootDir)) {
            return stream.filter(path -> path.toString().endsWith(".java"))
                .anyMatch(path -> {
                    try {
                        return Files.readString(path).contains(text);
                    } catch (IOException e) {
                        return false;
                    }
                });
        }
    }
}
