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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private static final Pattern PACKAGE_PATTERN = Pattern.compile("^\\s*package\\s+([\\w\\.]+)\\s*;\\s*$", Pattern.MULTILINE);

    @TempDir
    Path tempDir;

    @Test
    void generatesDelegatedArtifactsFromYamlWithoutAnnotatedGlue() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-delegate-only.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "embed"
                operator: "com.example.lib.EmbeddingService"
            """);

        Path delegateSource = writeSource("EmbeddingService.java", """
            package com.example.lib;

            import io.smallrye.mutiny.Uni;
            import org.pipelineframework.service.ReactiveService;

            public class EmbeddingService implements ReactiveService<LibraryChunk, LibraryVector> {
                @Override
                public Uni<LibraryVector> process(LibraryChunk input) {
                    return Uni.createFrom().item(new LibraryVector());
                }
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
        Path uniStub = writeUniStub();
        Path markerSource = writeSource("Marker.java", """
            package com.example.app;

            public class Marker {
            }
            """);

        CompilationResult result = compile(yamlFile, List.of(delegateSource, libInput, libOutput, uniStub, markerSource));
        assertTrue(result.success(), "Expected delegated YAML step compilation to succeed: " + result.errorSummary());

    }

    @Test
    void validatesDelegatedYamlStepContractsDeterministically() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "embed"
                operator: "com.example.lib.EmbeddingService"
                input: "com.example.app.TextChunk"
                output: "com.example.app.Vector"
                operatorMapper: "com.example.app.EmbeddingMapper"
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

            public interface ExternalMapper<TApplicationInput, TOperatorInput, TApplicationOutput, TOperatorOutput> {
                TOperatorInput toOperatorInput(TApplicationInput input);

                TApplicationOutput toApplicationOutput(TOperatorOutput output);
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

        assertFalse(result.success(), "Expected deterministic contract validation failure in this harness");
        String errors = result.errorSummary();
        assertTrue(errors.contains("Delegate service 'com.example.lib.EmbeddingService'"),
            "Expected delegated-service validation diagnostic: " + errors);
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
        assertFalse(result.success(), "Expected failure when YAML internal service lacks @PipelineStep");
        String errors = result.errorSummary().toLowerCase(Locale.ROOT);
        assertTrue(errors.contains("must be annotated with @pipelinestep"), result.errorSummary());
    }

    @Test
    void doesNotGenerateArtifactsForAnnotatedServicesNotReferencedInYaml() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-referenced-only.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import io.smallrye.mutiny.Uni;
            import org.pipelineframework.annotation.PipelineStep;
            import org.pipelineframework.service.ReactiveService;

            @PipelineStep(inputType = String.class, outputType = String.class)
            public class PaymentService implements ReactiveService<String, String> {
                @Override
                public Uni<String> process(String input) {
                    return Uni.createFrom().item(input);
                }
            }
            """);
        Path unusedAnnotatedService = writeSource("UnusedService.java", """
            package com.example.app;

            import io.smallrye.mutiny.Uni;
            import org.pipelineframework.annotation.PipelineStep;
            import org.pipelineframework.service.ReactiveService;

            @PipelineStep(inputType = String.class, outputType = String.class)
            public class UnusedService implements ReactiveService<String, String> {
                @Override
                public Uni<String> process(String input) {
                    return Uni.createFrom().item(input);
                }
            }
            """);
        Path markerSource = writeSource("Marker2.java", """
            package com.example.app;

            public class Marker2 {
            }
            """);

        Path uniStub = writeUniStub();
        CompilationResult result = compile(yamlFile, List.of(paymentService, unusedAnnotatedService, uniStub, markerSource));
        assertTrue(result.success(), "Expected YAML-referenced internal step compilation to succeed: " + result.errorSummary());

        String warnings = result.messagesOfKind(Diagnostic.Kind.WARNING).toLowerCase(Locale.ROOT);
        assertTrue(warnings.contains("unusedservice"), "Expected warning about unreferenced @PipelineStep service: " + warnings);
        assertTrue(warnings.contains("not referenced in pipeline yaml"),
            "Expected unreferenced-service policy warning in diagnostics: " + warnings);
    }

    @Test
    void infersDelegatedExternalMapperWhenExactlyOneCandidateMatches() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-delegate-infer-mapper.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "embed"
                operator: "com.example.lib.EmbeddingService3"
                input: "com.example.app.TextChunk3"
                output: "com.example.app.Vector3"
            """);

        Path delegateSource = writeSource("EmbeddingService3.java", """
            package com.example.lib;

            import io.smallrye.mutiny.Uni;
            import org.pipelineframework.service.ReactiveService;

            public class EmbeddingService3 implements ReactiveService<LibraryChunk3, LibraryVector3> {
                @Override
                public Uni<LibraryVector3> process(LibraryChunk3 input) {
                    return Uni.createFrom().item(new LibraryVector3());
                }
            }
            """);
        Path libInput = writeSource("LibraryChunk3.java", """
            package com.example.lib;

            public class LibraryChunk3 {
            }
            """);
        Path libOutput = writeSource("LibraryVector3.java", """
            package com.example.lib;

            public class LibraryVector3 {
            }
            """);
        Path appInput = writeSource("TextChunk3.java", """
            package com.example.app;

            public class TextChunk3 {
            }
            """);
        Path appOutput = writeSource("Vector3.java", """
            package com.example.app;

            public class Vector3 {
            }
            """);
        Path inferredMapper = writeSource("EmbeddingMapper3.java", """
            package com.example.app;

            import com.example.lib.LibraryChunk3;
            import com.example.lib.LibraryVector3;
            import org.pipelineframework.mapper.ExternalMapper;

            public class EmbeddingMapper3 implements ExternalMapper<TextChunk3, LibraryChunk3, Vector3, LibraryVector3> {
                @Override
                public LibraryChunk3 toOperatorInput(TextChunk3 input) {
                    return new LibraryChunk3();
                }

                @Override
                public Vector3 toApplicationOutput(LibraryVector3 output) {
                    return new Vector3();
                }
            }
            """);
        Path externalMapperContract = writeSource("ExternalMapper.java", """
            package org.pipelineframework.mapper;

            public interface ExternalMapper<TApplicationInput, TOperatorInput, TApplicationOutput, TOperatorOutput> {
                TOperatorInput toOperatorInput(TApplicationInput input);

                TApplicationOutput toApplicationOutput(TOperatorOutput output);
            }
            """);
        Path uniStub = writeUniStub();
        Path markerSource = writeSource("Marker4.java", """
            package com.example.app;

            public class Marker4 {
            }
            """);

        CompilationResult result = compile(yamlFile, List.of(
            delegateSource, libInput, libOutput, appInput, appOutput, inferredMapper, externalMapperContract, uniStub, markerSource));
        assertTrue(result.success(), "Expected delegated external mapper inference to succeed: " + result.errorSummary());
    }

    @Test
    void allowsDelegatedStepWithoutMapperWhenJacksonFallbackEnabled() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-delegate-fallback.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "fallback-embed"
                operator: "com.example.lib.FallbackEmbeddingService"
                input: "com.example.app.FallbackInput"
                output: "com.example.app.FallbackOutput"
                mapperFallback: "JACKSON"
            """);

        Path delegateSource = writeSource("FallbackEmbeddingService.java", """
            package com.example.lib;

            import io.smallrye.mutiny.Uni;
            import org.pipelineframework.service.ReactiveService;

            public class FallbackEmbeddingService implements ReactiveService<LibraryFallbackInput, LibraryFallbackOutput> {
                @Override
                public Uni<LibraryFallbackOutput> process(LibraryFallbackInput input) {
                    return Uni.createFrom().item(new LibraryFallbackOutput());
                }
            }
            """);
        Path libInput = writeSource("LibraryFallbackInput.java", """
            package com.example.lib;

            public class LibraryFallbackInput {
            }
            """);
        Path libOutput = writeSource("LibraryFallbackOutput.java", """
            package com.example.lib;

            public class LibraryFallbackOutput {
            }
            """);
        Path appInput = writeSource("FallbackInput.java", """
            package com.example.app;

            public class FallbackInput {
            }
            """);
        Path appOutput = writeSource("FallbackOutput.java", """
            package com.example.app;

            public class FallbackOutput {
            }
            """);
        Path uniStub = writeUniStub();

        CompilationResult result = compile(
            yamlFile,
            List.of(delegateSource, libInput, libOutput, appInput, appOutput, uniStub),
            List.of("-Apipeline.mapper.fallback.enabled=true"));

        assertTrue(result.success(), "Expected delegated step fallback compilation to succeed: " + result.errorSummary());
        String warnings = result.messagesOfKind(Diagnostic.Kind.WARNING);
        assertFalse(warnings.contains("Skipping delegated step 'fallback-embed'"),
            "Expected delegated step not to be skipped when mapper fallback is enabled: " + warnings);
    }

    private Path writeSource(String fileName, String content) throws IOException {
        Path baseDir = tempDir;
        Matcher matcher = PACKAGE_PATTERN.matcher(content);
        if (matcher.find()) {
            String packageName = matcher.group(1);
            baseDir = tempDir.resolve(packageName.replace('.', java.io.File.separatorChar));
        }
        Path file = baseDir.resolve(fileName);
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
        return file;
    }

    private Path writeUniStub() throws IOException {
        return writeSource("Uni.java", """
            package io.smallrye.mutiny;

            public class Uni<T> {
                public static CreateFrom createFrom() {
                    return new CreateFrom();
                }

                public static class CreateFrom {
                    public <U> Uni<U> item(U value) {
                        return new Uni<>();
                    }
                }
            }
            """);
    }

    private CompilationResult compile(Path yamlFile, List<Path> sources) throws IOException {
        return compile(yamlFile, sources, List.of());
    }

    private CompilationResult compile(Path yamlFile, List<Path> sources, List<String> extraOptions) throws IOException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler, "JavaCompiler should be available");
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        Path generatedDir = tempDir.resolve("generated");
        Path classesDir = tempDir.resolve("classes");
        Files.createDirectories(generatedDir);
        Files.createDirectories(classesDir);

        try (StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null)) {
            List<String> options = new ArrayList<>(List.of(
                "-proc:only",
                "-s", generatedDir.toString(),
                "-d", classesDir.toString(),
                "-classpath", System.getProperty("java.class.path"),
                "-processor", "org.pipelineframework.processor.PipelineStepProcessor",
                "-Apipeline.config=" + yamlFile
            ));
            options.addAll(extraOptions);

            JavaCompiler.CompilationTask task = compiler.getTask(
                null,
                fileManager,
                diagnostics,
                options,
                null,
                fileManager.getJavaFileObjectsFromPaths(sources)
            );
            boolean success = Boolean.TRUE.equals(task.call());
            return new CompilationResult(success, diagnostics.getDiagnostics());
        }
    }

    private record CompilationResult(boolean success, List<Diagnostic<? extends JavaFileObject>> diagnostics) {
        private String errorSummary() {
            List<String> errors = new ArrayList<>();
            for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics) {
                if (diagnostic.getKind() == Diagnostic.Kind.ERROR) {
                    errors.add(diagnostic.getMessage(null));
                }
            }
            return String.join(" | ", errors);
        }

        private String messagesOfKind(Diagnostic.Kind kind) {
            List<String> messages = new ArrayList<>();
            for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics) {
                if (diagnostic.getKind() == kind) {
                    messages.add(diagnostic.getMessage(null));
                }
            }
            return String.join(" | ", messages);
        }
    }
}
