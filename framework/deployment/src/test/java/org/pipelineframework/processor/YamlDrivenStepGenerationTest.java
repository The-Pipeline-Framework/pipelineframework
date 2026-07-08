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

        assertTrue(result.success(), "Expected delegated YAML step with explicit mapper to compile: " + result.errorSummary());
    }

    @Test
    void generatesYamlInternalStepWithoutPipelineStepAnnotation() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-yaml-only.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import io.smallrye.mutiny.Uni;
            import org.pipelineframework.service.ReactiveService;

            public class PaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {
                @Override
                public Uni<PaymentStatus> process(PaymentRecord input) {
                    return Uni.createFrom().item(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writeSource("PaymentRecord.java", """
            package com.example.app;

            public class PaymentRecord {
            }
            """);
        Path paymentStatus = writeSource("PaymentStatus.java", """
            package com.example.app;

            public class PaymentStatus {
            }
            """);
        Path uniStub = writeUniStub();

        CompilationResult result = compile(yamlFile, List.of(paymentService, paymentRecord, paymentStatus, uniStub));
        assertTrue(result.success(), "Expected annotation-free YAML internal service compilation to succeed: " + result.errorSummary());
    }

    @Test
    void generatesYamlInternalStepFromPlainUniProcessMethod() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-plain-uni.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import io.smallrye.mutiny.Uni;

            public class PaymentService {
                public Uni<PaymentStatus> process(PaymentRecord input) {
                    return Uni.createFrom().item(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writeSource("PaymentRecord.java", """
            package com.example.app;

            public class PaymentRecord {
            }
            """);
        Path paymentStatus = writeSource("PaymentStatus.java", """
            package com.example.app;

            public class PaymentStatus {
            }
            """);
        Path uniStub = writeUniStub();

        CompilationResult result = compile(yamlFile, List.of(paymentService, paymentRecord, paymentStatus, uniStub));
        assertTrue(result.success(), "Expected plain Uni process method compilation to succeed: " + result.errorSummary());
    }

    @Test
    void generatesYamlInternalStepFromPlainMonoProcessMethodWithSpringProfile() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-plain-mono.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import reactor.core.publisher.Mono;

            public class PaymentService {
                public Mono<PaymentStatus> process(PaymentRecord input) {
                    return Mono.just(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writeSource("PaymentRecord.java", """
            package com.example.app;

            public class PaymentRecord {
            }
            """);
        Path paymentStatus = writeSource("PaymentStatus.java", """
            package com.example.app;

            public class PaymentStatus {
            }
            """);
        Path monoStub = writeMonoStub();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentService, paymentRecord, paymentStatus, monoStub),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertTrue(result.success(), "Expected plain Mono process method compilation to succeed: " + result.errorSummary());
    }

    @Test
    void generatesYamlInternalStepFromPlainCompletionStageProcessMethodWithSpringProfile() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-plain-stage.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import java.util.concurrent.CompletableFuture;
            import java.util.concurrent.CompletionStage;

            public class PaymentService {
                public CompletionStage<PaymentStatus> process(PaymentRecord input) {
                    return CompletableFuture.completedFuture(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writePaymentRecord();
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentService, paymentRecord, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertTrue(result.success(), "Expected plain CompletionStage process method compilation to succeed: "
            + result.errorSummary());
    }

    @Test
    void generatesYamlDelegatedStepFromPlainMonoProcessMethodWithSpringProfile() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-delegated-plain-mono.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "audit"
                operator: "com.example.app.PaymentAuditService"
                input: "com.example.app.PaymentStatus"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentAuditService = writeSource("PaymentAuditService.java", """
            package com.example.app;

            import reactor.core.publisher.Mono;

            public class PaymentAuditService {
                public Mono<PaymentStatus> process(PaymentStatus input) {
                    return Mono.just(new PaymentStatus());
                }
            }
            """);
        Path paymentStatus = writePaymentStatus();
        Path monoStub = writeMonoStub();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentAuditService, paymentStatus, monoStub),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertTrue(result.success(), "Expected Spring delegated Mono operator compilation to succeed: " + result.errorSummary());
    }

    @Test
    void generatesSpringDelegatedStepFromNamedMonoOperatorMethod() throws IOException {
        Path yamlFile = writeSpringNamedOperatorYaml(
            "pipeline-delegated-named-mono.yaml",
            "com.example.app.PaymentAuditService::audit",
            "");
        Path paymentAuditService = writePaymentAuditService("""
            import reactor.core.publisher.Mono;

            public class PaymentAuditService {
                public Mono<PaymentStatus> audit(PaymentStatus input) {
                    return Mono.just(new PaymentStatus());
                }
            }
            """);
        Path paymentStatus = writePaymentStatus();
        Path monoStub = writeMonoStub();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentAuditService, paymentStatus, monoStub),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertTrue(result.success(), "Expected Spring named Mono operator compilation to succeed: " + result.errorSummary());
    }

    @Test
    void generatesSpringDelegatedStepFromNamedCompletionStageOperatorMethod() throws IOException {
        Path yamlFile = writeSpringNamedOperatorYaml(
            "pipeline-delegated-named-stage.yaml",
            "com.example.app.PaymentAuditService::auditAsync",
            "");
        Path paymentAuditService = writePaymentAuditService("""
            import java.util.concurrent.CompletableFuture;
            import java.util.concurrent.CompletionStage;

            public class PaymentAuditService {
                public CompletionStage<PaymentStatus> auditAsync(PaymentStatus input) {
                    return CompletableFuture.completedFuture(new PaymentStatus());
                }
            }
            """);
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentAuditService, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertTrue(result.success(), "Expected Spring named CompletionStage operator compilation to succeed: "
            + result.errorSummary());
    }

    @Test
    void generatesSpringDelegatedStepFromNamedBlockingOperatorMethod() throws IOException {
        Path yamlFile = writeSpringNamedOperatorYaml(
            "pipeline-delegated-named-blocking.yaml",
            "com.example.app.PaymentAuditService::auditBlocking",
            "");
        Path paymentAuditService = writePaymentAuditService("""
            public class PaymentAuditService {
                public PaymentStatus auditBlocking(PaymentStatus input) {
                    return new PaymentStatus();
                }
            }
            """);
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentAuditService, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertTrue(result.success(), "Expected Spring named blocking operator compilation to succeed: "
            + result.errorSummary());
    }

    @Test
    void failsNamedOperatorMethodWithoutSpringProfile() throws IOException {
        Path yamlFile = writeSpringNamedOperatorYaml(
            "pipeline-delegated-named-default-profile.yaml",
            "com.example.app.PaymentAuditService::audit",
            "");
        Path paymentAuditService = writePaymentAuditService("""
            public class PaymentAuditService {
                public PaymentStatus audit(PaymentStatus input) {
                    return new PaymentStatus();
                }
            }
            """);
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(yamlFile, List.of(paymentAuditService, paymentStatus));
        assertFalse(result.success(), "Expected default renderer profile to reject Class::method delegated operators");
        assertTrue(result.errorSummary().contains("only by the Spring renderer profile"), result.errorSummary());
    }

    @Test
    void failsNamedOperatorMethodWhenMissing() throws IOException {
        Path yamlFile = writeSpringNamedOperatorYaml(
            "pipeline-delegated-named-missing.yaml",
            "com.example.app.PaymentAuditService::audit",
            "");
        Path paymentAuditService = writePaymentAuditService("""
            public class PaymentAuditService {
                public PaymentStatus process(PaymentStatus input) {
                    return new PaymentStatus();
                }
            }
            """);
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentAuditService, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected missing named Spring operator method to fail");
        assertTrue(result.errorSummary().contains("was not found"), result.errorSummary());
    }

    @Test
    void failsNamedOperatorMethodWhenOverloaded() throws IOException {
        Path yamlFile = writeSpringNamedOperatorYaml(
            "pipeline-delegated-named-overloaded.yaml",
            "com.example.app.PaymentAuditService::audit",
            "");
        Path paymentAuditService = writePaymentAuditService("""
            public class PaymentAuditService {
                public PaymentStatus audit(PaymentStatus input) {
                    return new PaymentStatus();
                }

                public PaymentStatus audit(AlternatePaymentStatus input) {
                    return new PaymentStatus();
                }
            }
            """);
        Path paymentStatus = writePaymentStatus();
        Path alternatePaymentStatus = writeSource("AlternatePaymentStatus.java", """
            package com.example.app;

            public class AlternatePaymentStatus {
            }
            """);

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentAuditService, paymentStatus, alternatePaymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected overloaded named Spring operator method to fail");
        assertTrue(result.errorSummary().contains("is overloaded"), result.errorSummary());
    }

    @Test
    void failsNamedOperatorMethodWhenWrongArity() throws IOException {
        Path yamlFile = writeSpringNamedOperatorYaml(
            "pipeline-delegated-named-wrong-arity.yaml",
            "com.example.app.PaymentAuditService::audit",
            "");
        Path paymentAuditService = writePaymentAuditService("""
            public class PaymentAuditService {
                public PaymentStatus audit(PaymentStatus input, String reason) {
                    return new PaymentStatus();
                }
            }
            """);
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentAuditService, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected wrong-arity named Spring operator method to fail");
        assertTrue(result.errorSummary().contains("exactly one input parameter"), result.errorSummary());
    }

    @Test
    void failsNamedOperatorMethodWhenVoid() throws IOException {
        Path yamlFile = writeSpringNamedOperatorYaml(
            "pipeline-delegated-named-void.yaml",
            "com.example.app.PaymentAuditService::audit",
            "");
        Path paymentAuditService = writePaymentAuditService("""
            public class PaymentAuditService {
                public void audit(PaymentStatus input) {
                }
            }
            """);
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentAuditService, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected void named Spring operator method to fail");
        assertTrue(result.errorSummary().contains("must return an output value"), result.errorSummary());
    }

    @Test
    void failsNamedOperatorMethodWhenStaticOnly() throws IOException {
        Path yamlFile = writeSpringNamedOperatorYaml(
            "pipeline-delegated-named-static.yaml",
            "com.example.app.PaymentAuditService::audit",
            "");
        Path paymentAuditService = writePaymentAuditService("""
            public class PaymentAuditService {
                public static PaymentStatus audit(PaymentStatus input) {
                    return new PaymentStatus();
                }
            }
            """);
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentAuditService, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected static named Spring operator method to fail");
        assertTrue(result.errorSummary().contains("must be instance bean methods"), result.errorSummary());
    }

    @Test
    void failsNamedOperatorMethodWhenReturnTypeIsUnsupportedReactiveWrapper() throws IOException {
        Path yamlFile = writeSpringNamedOperatorYaml(
            "pipeline-delegated-named-flux.yaml",
            "com.example.app.PaymentAuditService::audit",
            "");
        Path paymentAuditService = writePaymentAuditService("""
            import reactor.core.publisher.Flux;

            public class PaymentAuditService {
                public Flux<PaymentStatus> audit(PaymentStatus input) {
                    return Flux.just(new PaymentStatus());
                }
            }
            """);
        Path paymentStatus = writePaymentStatus();
        Path fluxStub = writeFluxStub();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentAuditService, paymentStatus, fluxStub),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected unsupported reactive named Spring operator return type to fail");
        assertTrue(result.errorSummary().contains("does not support return type"), result.errorSummary());
    }

    @Test
    void failsNamedOperatorMethodWhenCardinalityIsNonUnary() throws IOException {
        Path yamlFile = writeSpringNamedOperatorYaml(
            "pipeline-delegated-named-non-unary.yaml",
            "com.example.app.PaymentAuditService::audit",
            "    cardinality: \"ONE_TO_MANY\"\n");
        Path paymentAuditService = writePaymentAuditService("""
            public class PaymentAuditService {
                public PaymentStatus audit(PaymentStatus input) {
                    return new PaymentStatus();
                }
            }
            """);
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentAuditService, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected non-unary named Spring operator compilation to fail");
        assertTrue(result.errorSummary().contains("declares cardinality"), result.errorSummary());
    }

    @Test
    void failsYamlInternalStepFromPlainMonoProcessMethodWithoutSpringProfile() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-plain-mono-quarkus.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import reactor.core.publisher.Mono;

            public class PaymentService {
                public Mono<PaymentStatus> process(PaymentRecord input) {
                    return Mono.just(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writeSource("PaymentRecord.java", """
            package com.example.app;

            public class PaymentRecord {
            }
            """);
        Path paymentStatus = writeSource("PaymentStatus.java", """
            package com.example.app;

            public class PaymentStatus {
            }
            """);
        Path monoStub = writeMonoStub();

        CompilationResult result = compile(yamlFile, List.of(paymentService, paymentRecord, paymentStatus, monoStub));
        assertFalse(result.success(), "Expected default renderer profile to reject plain Mono process methods");
        assertTrue(
            result.errorSummary().contains("must implement exactly one supported service interface or declare exactly one public process(In): Uni<Out> method"),
            result.errorSummary());
    }

    @Test
    void failsYamlInternalStepFromPlainCompletionStageProcessMethodWithoutSpringProfile() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-plain-stage-quarkus.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import java.util.concurrent.CompletableFuture;
            import java.util.concurrent.CompletionStage;

            public class PaymentService {
                public CompletionStage<PaymentStatus> process(PaymentRecord input) {
                    return CompletableFuture.completedFuture(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writePaymentRecord();
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(yamlFile, List.of(paymentService, paymentRecord, paymentStatus));
        assertFalse(result.success(), "Expected default renderer profile to reject plain CompletionStage process methods");
        assertTrue(
            result.errorSummary().contains("must implement exactly one supported service interface or declare exactly one public process(In): Uni<Out> method"),
            result.errorSummary());
    }

    @Test
    void failsYamlInternalStepWhenPlainMonoProcessMethodIsRaw() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-raw-plain-mono.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import reactor.core.publisher.Mono;

            public class PaymentService {
                @SuppressWarnings("rawtypes")
                public Mono process(PaymentRecord input) {
                    return Mono.just(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writeSource("PaymentRecord.java", """
            package com.example.app;

            public class PaymentRecord {
            }
            """);
        Path paymentStatus = writeSource("PaymentStatus.java", """
            package com.example.app;

            public class PaymentStatus {
            }
            """);
        Path monoStub = writeMonoStub();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentService, paymentRecord, paymentStatus, monoStub),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected raw Mono process method compilation to fail");
        assertTrue(
            result.errorSummary().contains("process(In): Mono<Out>"),
            result.errorSummary());
    }

    @Test
    void failsYamlInternalStepWhenPlainCompletionStageProcessMethodIsRaw() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-raw-plain-stage.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import java.util.concurrent.CompletableFuture;
            import java.util.concurrent.CompletionStage;

            public class PaymentService {
                @SuppressWarnings("rawtypes")
                public CompletionStage process(PaymentRecord input) {
                    return CompletableFuture.completedFuture(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writePaymentRecord();
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentService, paymentRecord, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected raw CompletionStage process method compilation to fail");
        assertTrue(result.errorSummary().contains("process(In): CompletionStage<Out>"), result.errorSummary());
    }

    @Test
    void failsYamlInternalStepWhenPlainCompletionStageProcessMethodHasWrongArity() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-wrong-arity-plain-stage.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import java.util.concurrent.CompletableFuture;
            import java.util.concurrent.CompletionStage;

            public class PaymentService {
                public CompletionStage<PaymentStatus> process(PaymentRecord input, String unused) {
                    return CompletableFuture.completedFuture(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writePaymentRecord();
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentService, paymentRecord, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected wrong-arity CompletionStage process method compilation to fail");
        assertTrue(result.errorSummary().contains("process method must accept exactly one input parameter"),
            result.errorSummary());
        assertTrue(result.errorSummary().contains("process(In): CompletionStage<Out>"), result.errorSummary());
    }

    @Test
    void failsYamlInternalStepWhenPlainCompletionStageProcessMethodReturnsVoid() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-void-plain-stage.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            public class PaymentService {
                public void process(PaymentRecord input) {
                }
            }
            """);
        Path paymentRecord = writePaymentRecord();
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentService, paymentRecord, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected void process method compilation to fail");
        assertTrue(result.errorSummary().contains("process method must return a typed output"), result.errorSummary());
        assertTrue(result.errorSummary().contains("process(In): CompletionStage<Out>"), result.errorSummary());
    }

    @Test
    void failsYamlInternalStepWhenMultiplePlainReactiveProcessMethodsExist() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-multiple-plain-reactive.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import io.smallrye.mutiny.Uni;
            import reactor.core.publisher.Mono;

            public class PaymentService {
                public Uni<PaymentStatus> process(PaymentRecord input) {
                    return Uni.createFrom().item(new PaymentStatus());
                }

                public Mono<PaymentStatus> process(AlternatePaymentRecord input) {
                    return Mono.just(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writeSource("PaymentRecord.java", """
            package com.example.app;

            public class PaymentRecord {
            }
            """);
        Path alternatePaymentRecord = writeSource("AlternatePaymentRecord.java", """
            package com.example.app;

            public class AlternatePaymentRecord {
            }
            """);
        Path paymentStatus = writeSource("PaymentStatus.java", """
            package com.example.app;

            public class PaymentStatus {
            }
            """);
        Path uniStub = writeUniStub();
        Path monoStub = writeMonoStub();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentService, paymentRecord, alternatePaymentRecord, paymentStatus, uniStub, monoStub),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected multiple plain reactive process methods to fail");
        assertAll(
            () -> assertTrue(result.errorSummary().contains("multiple public"), result.errorSummary()),
            () -> assertTrue(result.errorSummary().contains("processBlocking(In): Out"), result.errorSummary()));
    }

    @Test
    void failsYamlInternalStepWhenMultiplePlainSpringProcessMethodsExist() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-multiple-plain-spring.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import java.util.concurrent.CompletableFuture;
            import java.util.concurrent.CompletionStage;
            import reactor.core.publisher.Mono;

            public class PaymentService {
                public Mono<PaymentStatus> process(PaymentRecord input) {
                    return Mono.just(new PaymentStatus());
                }

                public CompletionStage<PaymentStatus> process(AlternatePaymentRecord input) {
                    return CompletableFuture.completedFuture(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writePaymentRecord();
        Path alternatePaymentRecord = writeSource("AlternatePaymentRecord.java", """
            package com.example.app;

            public class AlternatePaymentRecord {
            }
            """);
        Path paymentStatus = writePaymentStatus();
        Path monoStub = writeMonoStub();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentService, paymentRecord, alternatePaymentRecord, paymentStatus, monoStub),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected multiple plain Spring process methods to fail");
        assertAll(
            () -> assertTrue(result.errorSummary().contains("multiple public"), result.errorSummary()),
            () -> assertTrue(result.errorSummary().contains("CompletionStage<Out>"), result.errorSummary()));
    }

    @Test
    void failsYamlInternalStepWhenCompletionStageCardinalityIsNonUnary() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-stage-non-unary.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                cardinality: "ONE_TO_MANY"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import java.util.concurrent.CompletableFuture;
            import java.util.concurrent.CompletionStage;

            public class PaymentService {
                public CompletionStage<PaymentStatus> process(PaymentRecord input) {
                    return CompletableFuture.completedFuture(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writePaymentRecord();
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentService, paymentRecord, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected non-unary CompletionStage process method compilation to fail");
        assertTrue(result.errorSummary().contains("declares cardinality"), result.errorSummary());
    }

    @Test
    void generatesYamlInternalStepFromPlainBlockingProcessMethodWithSpringProfile() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-plain-blocking.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeBlockingPaymentService();
        Path paymentRecord = writePaymentRecord();
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentService, paymentRecord, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertTrue(result.success(), "Expected plain blocking process method compilation to succeed: " + result.errorSummary());
    }

    @Test
    void generatesYamlInternalStepFromPlainBlockingProcessMethodWithVirtualThreads() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-plain-blocking-virtual.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
                runOnVirtualThreads: true
            """);

        Path paymentService = writeBlockingPaymentService();
        Path paymentRecord = writePaymentRecord();
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentService, paymentRecord, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertTrue(result.success(), "Expected virtual-thread blocking service compilation to succeed: " + result.errorSummary());
    }

    @Test
    void failsYamlInternalReactiveProcessMethodWithVirtualThreads() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-plain-uni-virtual.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
                runOnVirtualThreads: true
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import io.smallrye.mutiny.Uni;

            public class PaymentService {
                public Uni<PaymentStatus> process(PaymentRecord input) {
                    return Uni.createFrom().item(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writePaymentRecord();
        Path paymentStatus = writePaymentStatus();
        Path uniStub = writeUniStub();

        CompilationResult result = compile(yamlFile, List.of(paymentService, paymentRecord, paymentStatus, uniStub));
        assertFalse(result.success(), "Expected reactive virtual-thread service compilation to fail");
        assertAll(
            () -> assertTrue(result.errorSummary().contains("runOnVirtualThreads"), result.errorSummary()),
            () -> assertTrue(result.errorSummary().contains("blocking internal services"), result.errorSummary()));
    }

    @Test
    void failsYamlInternalStepFromPlainBlockingProcessMethodWithoutSpringProfile() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-plain-blocking-quarkus.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeBlockingPaymentService();
        Path paymentRecord = writePaymentRecord();
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(yamlFile, List.of(paymentService, paymentRecord, paymentStatus));
        assertFalse(result.success(), "Expected default renderer profile to reject plain blocking process methods");
        assertTrue(
            result.errorSummary().contains("must implement exactly one supported service interface or declare exactly one public process(In): Uni<Out> method"),
            result.errorSummary());
    }

    @Test
    void failsYamlInternalStepWhenPlainBlockingProcessMethodIsStatic() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-static-plain-blocking.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            public class PaymentService {
                public static PaymentStatus processBlocking(PaymentRecord input) {
                    return new PaymentStatus();
                }
            }
            """);
        Path paymentRecord = writePaymentRecord();
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentService, paymentRecord, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected static plain blocking process method compilation to fail");
        assertTrue(result.errorSummary().contains("processBlocking(In): Out"), result.errorSummary());
    }

    @Test
    void failsYamlInternalStepWhenPlainBlockingProcessMethodIsPrivate() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-private-plain-blocking.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            public class PaymentService {
                private PaymentStatus processBlocking(PaymentRecord input) {
                    return new PaymentStatus();
                }
            }
            """);
        Path paymentRecord = writePaymentRecord();
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentService, paymentRecord, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected private plain blocking process method compilation to fail");
        assertTrue(result.errorSummary().contains("processBlocking(In): Out"), result.errorSummary());
    }

    @Test
    void failsYamlInternalStepWhenPlainBlockingProcessMethodHasWrongArity() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-wrong-arity-plain-blocking.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            public class PaymentService {
                public PaymentStatus processBlocking(PaymentRecord input, String unused) {
                    return new PaymentStatus();
                }
            }
            """);
        Path paymentRecord = writePaymentRecord();
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentService, paymentRecord, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected wrong-arity plain blocking process method compilation to fail");
        assertTrue(result.errorSummary().contains("processBlocking(In): Out"), result.errorSummary());
    }

    @Test
    void failsYamlInternalStepWhenPlainBlockingProcessMethodReturnsVoid() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-void-plain-blocking.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            public class PaymentService {
                public void processBlocking(PaymentRecord input) {
                }
            }
            """);
        Path paymentRecord = writePaymentRecord();
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentService, paymentRecord, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected void plain blocking process method compilation to fail");
        assertTrue(result.errorSummary().contains("processBlocking(In): Out"), result.errorSummary());
    }

    @Test
    void failsYamlInternalStepWhenMultiplePlainBlockingProcessMethodsExist() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-multiple-plain-blocking.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            public class PaymentService {
                public PaymentStatus processBlocking(PaymentRecord input) {
                    return new PaymentStatus();
                }

                public PaymentStatus processBlocking(AlternatePaymentRecord input) {
                    return new PaymentStatus();
                }
            }
            """);
        Path paymentRecord = writePaymentRecord();
        Path alternatePaymentRecord = writeSource("AlternatePaymentRecord.java", """
            package com.example.app;

            public class AlternatePaymentRecord {
            }
            """);
        Path paymentStatus = writePaymentStatus();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentService, paymentRecord, alternatePaymentRecord, paymentStatus),
            List.of("-Apipeline.codegen.rendererProfile=spring"));
        assertFalse(result.success(), "Expected multiple plain blocking process methods to fail");
        assertAll(
            () -> assertTrue(result.errorSummary().contains("multiple public"), result.errorSummary()),
            () -> assertTrue(result.errorSummary().contains("processBlocking(In): Out"), result.errorSummary()));
    }

    @Test
    void failsYamlInternalStepWhenPlainUniProcessMethodIsStatic() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-static-plain-uni.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import io.smallrye.mutiny.Uni;

            public class PaymentService {
                public static Uni<PaymentStatus> process(PaymentRecord input) {
                    return Uni.createFrom().item(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writeSource("PaymentRecord.java", """
            package com.example.app;

            public class PaymentRecord {
            }
            """);
        Path paymentStatus = writeSource("PaymentStatus.java", """
            package com.example.app;

            public class PaymentStatus {
            }
            """);
        Path uniStub = writeUniStub();

        CompilationResult result = compile(yamlFile, List.of(paymentService, paymentRecord, paymentStatus, uniStub));
        assertFalse(result.success(), "Expected static plain Uni process method compilation to fail");
        assertTrue(
            result.errorSummary().contains("must implement exactly one supported service interface or declare exactly one public process(In): Uni<Out> method"),
            result.errorSummary());
    }

    @Test
    void failsYamlInternalStepWhenServiceClassIsMissing() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-missing-internal.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.MissingPaymentService"
            """);

        Path markerSource = writeSource("MarkerMissingService.java", """
            package com.example.app;

            public class MarkerMissingService {
            }
            """);

        CompilationResult result = compile(yamlFile, List.of(markerSource));
        assertFalse(result.success(), "Expected missing YAML internal service class to fail");
        assertTrue(result.errorSummary().contains("not found for step 'pay'"), result.errorSummary());
    }

    @Test
    void failsYamlInternalStepWhenServiceDoesNotImplementSupportedContract() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-bad-internal-shape.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
            """);

        Path nonService = writeSource("PaymentService.java", """
            package com.example.app;

            public class PaymentService {
                public String process(String input) {
                    return input;
                }
            }
            """);

        CompilationResult result = compile(yamlFile, List.of(nonService));
        assertFalse(result.success(), "Expected unsupported internal service contract to fail");
        assertTrue(
            result.errorSummary().contains("must implement exactly one supported service interface or declare exactly one public process(In): Uni<Out> method"),
            result.errorSummary());
    }

    @Test
    void generatesInternalStepFromYamlOwnedTypesAndMappers() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-yaml-owned.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                cardinality: "ONE_TO_ONE"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
                inboundMapper: "com.example.app.PaymentRecordMapper"
                outboundMapper: "com.example.app.PaymentStatusMapper"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import io.smallrye.mutiny.Uni;
            import org.pipelineframework.service.ReactiveService;

            public class PaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {
                @Override
                public Uni<PaymentStatus> process(PaymentRecord input) {
                    return Uni.createFrom().item(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writeSource("PaymentRecord.java", """
            package com.example.app;

            public class PaymentRecord {
            }
            """);
        Path paymentStatus = writeSource("PaymentStatus.java", """
            package com.example.app;

            public class PaymentStatus {
            }
            """);
        Path recordDto = writeSource("PaymentRecordDto.java", """
            package com.example.app;

            public class PaymentRecordDto {
            }
            """);
        Path statusDto = writeSource("PaymentStatusDto.java", """
            package com.example.app;

            public class PaymentStatusDto {
            }
            """);
        Path inboundMapper = writeSource("PaymentRecordMapper.java", """
            package com.example.app;

            import org.pipelineframework.mapper.Mapper;

            public class PaymentRecordMapper implements Mapper<PaymentRecord, PaymentRecordDto> {
                @Override
                public PaymentRecord fromExternal(PaymentRecordDto external) {
                    return new PaymentRecord();
                }

                @Override
                public PaymentRecordDto toExternal(PaymentRecord domain) {
                    return new PaymentRecordDto();
                }
            }
            """);
        Path outboundMapper = writeSource("PaymentStatusMapper.java", """
            package com.example.app;

            import org.pipelineframework.mapper.Mapper;

            public class PaymentStatusMapper implements Mapper<PaymentStatus, PaymentStatusDto> {
                @Override
                public PaymentStatus fromExternal(PaymentStatusDto external) {
                    return new PaymentStatus();
                }

                @Override
                public PaymentStatusDto toExternal(PaymentStatus domain) {
                    return new PaymentStatusDto();
                }
            }
            """);
        Path uniStub = writeUniStub();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentService, paymentRecord, paymentStatus, recordDto, statusDto, inboundMapper, outboundMapper, uniStub));
        assertTrue(result.success(), "Expected YAML-owned internal service compilation to succeed: " + result.errorSummary());
    }

    @Test
    void failsYamlInternalStepWhenCardinalityConflictsWithReactiveInterface() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-bad-cardinality.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "expand"
                service: "com.example.app.PaymentService"
                cardinality: "ONE_TO_MANY"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import io.smallrye.mutiny.Uni;
            import org.pipelineframework.service.ReactiveService;

            public class PaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {
                @Override
                public Uni<PaymentStatus> process(PaymentRecord input) {
                    return Uni.createFrom().item(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writeSource("PaymentRecord.java", """
            package com.example.app;

            public class PaymentRecord {
            }
            """);
        Path paymentStatus = writeSource("PaymentStatus.java", """
            package com.example.app;

            public class PaymentStatus {
            }
            """);
        Path uniStub = writeUniStub();

        CompilationResult result = compile(yamlFile, List.of(paymentService, paymentRecord, paymentStatus, uniStub));
        assertFalse(result.success(), "Expected cardinality mismatch to fail");
        assertTrue(
            result.errorSummary().contains("declares cardinality"),
            "Expected cardinality diagnostic: " + result.errorSummary());
    }

    @Test
    void failsYamlInternalStepWhenMapperDomainTypeDoesNotMatchYamlType() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-bad-mapper.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                cardinality: "ONE_TO_ONE"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
                inboundMapper: "com.example.app.WrongMapper"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import io.smallrye.mutiny.Uni;
            import org.pipelineframework.service.ReactiveService;

            public class PaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {
                @Override
                public Uni<PaymentStatus> process(PaymentRecord input) {
                    return Uni.createFrom().item(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writeSource("PaymentRecord.java", """
            package com.example.app;

            public class PaymentRecord {
            }
            """);
        Path paymentStatus = writeSource("PaymentStatus.java", """
            package com.example.app;

            public class PaymentStatus {
            }
            """);
        Path wrongDomain = writeSource("WrongDomain.java", """
            package com.example.app;

            public class WrongDomain {
            }
            """);
        Path recordDto = writeSource("PaymentRecordDto.java", """
            package com.example.app;

            public class PaymentRecordDto {
            }
            """);
        Path wrongMapper = writeSource("WrongMapper.java", """
            package com.example.app;

            import org.pipelineframework.mapper.Mapper;

            public class WrongMapper implements Mapper<WrongDomain, PaymentRecordDto> {
                @Override
                public WrongDomain fromExternal(PaymentRecordDto external) {
                    return new WrongDomain();
                }

                @Override
                public PaymentRecordDto toExternal(WrongDomain domain) {
                    return new PaymentRecordDto();
                }
            }
            """);
        Path uniStub = writeUniStub();

        CompilationResult result = compile(
            yamlFile,
            List.of(paymentService, paymentRecord, paymentStatus, wrongDomain, recordDto, wrongMapper, uniStub));
        assertFalse(result.success(), "Expected mapper domain mismatch to fail");
        assertTrue(
            result.errorSummary().contains("must declare Mapper<"),
            "Expected mapper mismatch diagnostic: " + result.errorSummary());
    }

    @Test
    void failsYamlInternalStepWhenYamlTypesConflictWithServiceContract() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-bad-type.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import io.smallrye.mutiny.Uni;
            import org.pipelineframework.service.ReactiveService;

            public class PaymentService implements ReactiveService<WrongRecord, PaymentStatus> {
                @Override
                public Uni<PaymentStatus> process(WrongRecord input) {
                    return Uni.createFrom().item(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writeSource("PaymentRecord.java", """
            package com.example.app;

            public class PaymentRecord {
            }
            """);
        Path wrongRecord = writeSource("WrongRecord.java", """
            package com.example.app;

            public class WrongRecord {
            }
            """);
        Path paymentStatus = writeSource("PaymentStatus.java", """
            package com.example.app;

            public class PaymentStatus {
            }
            """);
        Path uniStub = writeUniStub();

        CompilationResult result = compile(yamlFile, List.of(paymentService, paymentRecord, wrongRecord, paymentStatus, uniStub));
        assertFalse(result.success(), "Expected YAML/code type mismatch to fail");
        assertTrue(
            result.errorSummary().contains("declares input type 'com.example.app.PaymentRecord' in YAML"),
            "Expected YAML/code type mismatch diagnostic: " + result.errorSummary());
    }

    @Test
    void warnsWhenYamlOverridesDeprecatedPipelineStepMetadata() throws IOException {
        Path yamlFile = tempDir.resolve("pipeline-internal-annotation-secondary.yaml");
        Files.writeString(yamlFile, """
            appName: "Test App"
            basePackage: "com.example"
            transport: "LOCAL"
            steps:
              - name: "pay"
                service: "com.example.app.PaymentService"
                input: "com.example.app.PaymentRecord"
                output: "com.example.app.PaymentStatus"
            """);

        Path paymentService = writeSource("PaymentService.java", """
            package com.example.app;

            import io.smallrye.mutiny.Uni;
            import org.pipelineframework.annotation.PipelineStep;
            import org.pipelineframework.service.ReactiveService;

            @PipelineStep(inputType = String.class, outputType = String.class)
            public class PaymentService implements ReactiveService<PaymentRecord, PaymentStatus> {
                @Override
                public Uni<PaymentStatus> process(PaymentRecord input) {
                    return Uni.createFrom().item(new PaymentStatus());
                }
            }
            """);
        Path paymentRecord = writeSource("PaymentRecord.java", """
            package com.example.app;

            public class PaymentRecord {
            }
            """);
        Path paymentStatus = writeSource("PaymentStatus.java", """
            package com.example.app;

            public class PaymentStatus {
            }
            """);
        Path uniStub = writeUniStub();

        CompilationResult result = compile(yamlFile, List.of(paymentService, paymentRecord, paymentStatus, uniStub));
        assertTrue(result.success(), "Expected YAML to override annotation metadata without failing: " + result.errorSummary());
        String warnings = result.messagesOfKind(Diagnostic.Kind.WARNING);
        assertTrue(warnings.contains("YAML is authoritative"), "Expected YAML-authoritative warning: " + warnings);
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

    private Path writeSpringNamedOperatorYaml(String fileName, String operatorReference, String extraStepLines)
        throws IOException {
        Path yamlFile = tempDir.resolve(fileName);
        Files.writeString(yamlFile,
            "appName: \"Test App\"\n"
                + "basePackage: \"com.example\"\n"
                + "transport: \"LOCAL\"\n"
                + "steps:\n"
                + "  - name: \"audit\"\n"
                + "    operator: \"" + operatorReference + "\"\n"
                + "    input: \"com.example.app.PaymentStatus\"\n"
                + "    output: \"com.example.app.PaymentStatus\"\n"
                + extraStepLines);
        return yamlFile;
    }

    private Path writePaymentAuditService(String classBody) throws IOException {
        return writeSource("PaymentAuditService.java", """
            package com.example.app;

            %s
            """.formatted(classBody));
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

    private Path writeMonoStub() throws IOException {
        return writeSource("Mono.java", """
            package reactor.core.publisher;

            public class Mono<T> {
                public static <U> Mono<U> just(U value) {
                    return new Mono<>();
                }
            }
            """);
    }

    private Path writeFluxStub() throws IOException {
        return writeSource("Flux.java", """
            package reactor.core.publisher;

            public class Flux<T> {
                public static <U> Flux<U> just(U value) {
                    return new Flux<>();
                }
            }
            """);
    }

    private Path writeBlockingPaymentService() throws IOException {
        return writeSource("PaymentService.java", """
            package com.example.app;

            public class PaymentService {
                public PaymentStatus processBlocking(PaymentRecord input) {
                    return new PaymentStatus();
                }
            }
            """);
    }

    private Path writePaymentRecord() throws IOException {
        return writeSource("PaymentRecord.java", """
            package com.example.app;

            public class PaymentRecord {
            }
            """);
    }

    private Path writePaymentStatus() throws IOException {
        return writeSource("PaymentStatus.java", """
            package com.example.app;

            public class PaymentStatus {
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
        Path generatedDir = tempDir.resolve("target").resolve("generated-sources").resolve("pipeline");
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

    private record CompilationResult(
        boolean success,
        List<Diagnostic<? extends JavaFileObject>> diagnostics
    ) {
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
