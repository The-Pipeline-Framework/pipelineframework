package org.pipelineframework.processor;

import java.io.IOException;
import java.io.UncheckedIOException;
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
                service: "com.example.ProcessTestService"
                input: "java.lang.String"
                output: "java.lang.String"
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
            .compile(
                JavaFileObjects.forSourceString(
                    "com.example.PersistencePluginHost",
                    """
                        package com.example;

                        import org.pipelineframework.annotation.PipelinePlugin;

                        @PipelinePlugin("persistence")
                        public class PersistencePluginHost {
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.ProcessTestService",
                    """
                        package com.example;

                        import org.pipelineframework.annotation.PipelineStep;
                        import org.pipelineframework.service.ReactiveService;
                        import org.pipelineframework.step.StepOneToOne;
                        import io.smallrye.mutiny.Uni;
                        import jakarta.enterprise.context.ApplicationScoped;

                        @PipelineStep(
                            inputType = String.class,
                            outputType = String.class,
                            stepType = StepOneToOne.class,
                            inboundMapper = com.example.StringInputMapper.class,
                            outboundMapper = com.example.StringOutputMapper.class
                        )
                        @ApplicationScoped
	                        public class ProcessTestService implements ReactiveService<String, String> {
	                            @Override
	                            public Uni<String> process(String input) {
	                                return Uni.createFrom().item(input);
	                            }
	                        }
	                        """),
                JavaFileObjects.forSourceString(
                    "com.example.StringInputMapper",
                    """
                        package com.example;

                        public class StringInputMapper {
                            public String fromDto(String dto) {
                                return dto;
                            }

                            public String toDto(String domain) {
                                return domain;
                            }
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.StringOutputMapper",
                    """
                        package com.example;

                        public class StringOutputMapper {
                            public String fromDto(String dto) {
                                return dto;
                            }

                            public String toDto(String domain) {
                                return domain;
                            }
                        }
                        """),
                JavaFileObjects.forSourceString(
                    "com.example.PersistenceService",
                    """
                        package com.example;

                        public class PersistenceService {
                        }
                        """));

        assertThat(compilation).succeeded();

        Path restServerDir = generatedSourcesDir.resolve("rest-server");
        assertTrue(
            Files.exists(restServerDir),
            "Expected generated REST server directory to exist: " + restServerDir);
        assertTrue(
            containsText(restServerDir, "PersistenceService"),
            "Expected generated REST server sources under " + restServerDir
                + " to contain text 'PersistenceService'");
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
                        throw new UncheckedIOException(e);
                    }
                });
        }
    }
}
