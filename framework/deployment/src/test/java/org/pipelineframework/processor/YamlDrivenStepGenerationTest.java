/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.processor;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Basic tests for YAML-driven pipeline step generation.
 * These tests verify that the annotation processor can handle pipeline config options.
 */
class YamlDrivenStepGenerationTest {

    @TempDir
    Path tempDir;

    @Test
    void validatesDelegatedYamlStepContractsDeterministically() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "embed"
                delegate: "com.example.lib.EmbeddingService"
                input: "com.example.app.TextChunk"
                output: "com.example.app.Vector"
                externalMapper: "com.example.app.EmbeddingMapper"
            """);

        Path triggerSource = writeSource("TriggerStep.java", """
            package com.example.app;

            import org.pipelineframework.annotation.PipelineStep;

            @PipelineStep(inputType = String.class, outputType = String.class)
            public class TriggerStep {
            }
            """);
        Path delegateSource = writeSource("EmbeddingService.java", """
            package com.example.lib;

            import org.pipelineframework.service.ReactiveService;

            public class EmbeddingService implements ReactiveService<LibraryChunk, LibraryVector> {
            }
            """);
        Path libInput = writeSource("LibraryChunk.java", """
            package com.example.lib;

            public class LibraryChunk {
            }
            """);
        Path libOutput = writeSource("LibraryVector.java", """
            package com.example.lib;

            public class LibraryVector {
            }
            """);
        Path appInput = writeSource("TextChunk.java", """
            package com.example.app;

            public class TextChunk {
            }
            """);
        Path appOutput = writeSource("Vector.java", """
            package com.example.app;

            public class Vector {
            }
            """);
        Path externalMapperContract = writeSource("ExternalMapper.java", """
            package org.pipelineframework.mapper;

            public interface ExternalMapper<TApplicationInput, TLibraryInput, TApplicationOutput, TLibraryOutput> {
                TLibraryInput toLibraryInput(TApplicationInput input);

                TApplicationOutput toApplicationOutput(TLibraryOutput output);
            }
            """);
        Path reactiveServiceContract = writeSource("ReactiveService.java", """
            package org.pipelineframework.service;

            public interface ReactiveService<TInput, TOutput> {
            }
            """);
        Path mapperSource = writeSource("EmbeddingMapper.java", """
            package com.example.app;

            import com.example.lib.LibraryChunk;
            import com.example.lib.LibraryVector;
            import org.pipelineframework.mapper.ExternalMapper;

            public class EmbeddingMapper implements ExternalMapper<TextChunk, LibraryChunk, Vector, LibraryVector> {
            }
            """);

        CompilationResult result = compile(yamlFile, List.of(
            triggerSource, delegateSource, libInput, libOutput, appInput, appOutput,
            externalMapperContract, reactiveServiceContract, mapperSource));

        assertFalse(result.success, "Expected deterministic contract validation failure in this harness");
        String errors = result.errorSummary();
        assertTrue(errors.contains("Delegate service 'com.example.lib.EmbeddingService'"),
            "Expected delegated-service validation diagnostic: " + errors);
        assertTrue(errors.contains("External mapper 'com.example.app.EmbeddingMapper'"),
            "Expected external-mapper validation diagnostic: " + errors);
    }

    @Test
    void failsYamlInternalStepWhenServiceIsNotAnnotated() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-invalid-internal.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
            """);

        Path triggerSource = writeSource("TriggerStep2.java", """
            package com.example.app;

            import org.pipelineframework.annotation.PipelineStep;

            @PipelineStep(inputType = String.class, outputType = String.class)
            public class TriggerStep2 {
            }
            """);
        Path nonAnnotatedService = writeSource("PaymentService.java", """
            package com.example.app;

            public class PaymentService {
            }
            """);

        CompilationResult result = compile(yamlFile, List.of(triggerSource, nonAnnotatedService));
        assertFalse(result.success, "Expected failure when YAML internal service lacks @PipelineStep");
        String errors = result.errorSummary().toLowerCase(Locale.ROOT);
        assertTrue(errors.contains("must be annotated with @pipelinestep"), result.errorSummary());
    }

    private Path writeSource(String fileName, String content) throws IOException {
        Path file = tempDir.resolve(fileName);
        Files.writeString(file, content);
        return file;
    }

    private CompilationResult compile(Path yamlFile, List<Path> sources) throws IOException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler, "JavaCompiler should be available");
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        Path generatedDir = tempDir.resolve("generated");
        Files.createDirectories(generatedDir);

        try (StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null)) {
            JavaCompiler.CompilationTask task = compiler.getTask(
                null,
                fileManager,
                diagnostics,
                List.of(
                    "-proc:only",
                    "-s", generatedDir.toString(),
                    "-processor", "org.pipelineframework.processor.PipelineStepProcessor",
                    "-Apipeline.config=" + yamlFile
                ),
                null,
                fileManager.getJavaFileObjectsFromPaths(sources)
            );
            boolean success = Boolean.TRUE.equals(task.call());
            return new CompilationResult(success, diagnostics.getDiagnostics());
        }
    }

    private static final class CompilationResult {
        private final boolean success;
        private final List<Diagnostic<? extends JavaFileObject>> diagnostics;

        private CompilationResult(boolean success, List<Diagnostic<? extends JavaFileObject>> diagnostics) {
            this.success = success;
            this.diagnostics = diagnostics;
        }

        private String errorSummary() {
            List<String> errors = new ArrayList<>();
            for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics) {
                if (diagnostic.getKind() == Diagnostic.Kind.ERROR) {
                    errors.add(diagnostic.getMessage(null));
                }
            }
            return String.join(" | ", errors);
        }
    }
}
