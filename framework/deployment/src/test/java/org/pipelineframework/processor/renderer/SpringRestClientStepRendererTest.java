/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

package org.pipelineframework.processor.renderer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;
import org.pipelineframework.processor.ir.ServiceApiKind;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SpringRestClientStepRendererTest {

    @TempDir
    Path tempDir;

    @Test
    void rendersSpringWebClientUnaryRestClientStep() throws Exception {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(Map.of());
        SpringRestClientStepRenderer renderer = new SpringRestClientStepRenderer();

        renderer.render(new RestBinding(model(StreamingShape.UNARY_UNARY, false), "/payments"),
            new GenerationContext(processingEnv, tempDir, DeploymentRole.ORCHESTRATOR_CLIENT, Set.of(), null, null));

        Path clientStep = tempDir.resolve("com/example/service/pipeline/PaymentRestClientStep.java");
        String source = Files.readString(clientStep);
        assertTrue(source.contains("import java.util.concurrent.CompletionStage;"));
        assertTrue(source.contains("import org.pipelineframework.runtime.core.PipelineUnaryStep;"));
        assertTrue(source.contains("import org.springframework.core.env.Environment;"));
        assertTrue(source.contains("import org.springframework.stereotype.Component;"));
        assertTrue(source.contains("import org.springframework.web.reactive.function.client.WebClient;"));
        assertTrue(source.contains("@Component"));
        assertTrue(source.contains("public class PaymentRestClientStep implements PipelineUnaryStep<PaymentRecord, PaymentStatus>"));
        assertTrue(source.contains("private final WebClient webClient;"));
        assertTrue(source.contains("private final String endpointUrl;"));
        assertTrue(source.contains("Mapper<PaymentRecord, PaymentRecordDto> inboundMapper;"));
        assertTrue(source.contains("Mapper<PaymentStatus, PaymentStatusDto> outboundMapper;"));
        assertTrue(source.contains("public PaymentRestClientStep(WebClient.Builder webClientBuilder, Environment environment,"));
        assertTrue(source.contains("Mapper<PaymentRecord, PaymentRecordDto> inboundMapper,"));
        assertTrue(source.contains("Mapper<PaymentStatus, PaymentStatusDto> outboundMapper)"));
        assertTrue(source.contains("this.webClient = webClientBuilder.build();"));
        assertTrue(source.contains("PaymentRecordDto inputDto = this.inboundMapper.toExternal(input);"));
        assertTrue(source.contains(".bodyToMono(PaymentStatusDto.class)"));
        assertTrue(source.contains(".switchIfEmpty(Mono.error(new IllegalStateException(\"REST client step received an empty response body\")))"));
        assertTrue(source.contains(".map(this.outboundMapper::fromExternal)"));
        assertTrue(source.contains(".toFuture()"));
        assertTrue(source.contains("this.endpointUrl = resolveEndpointUrl(environment);"));
        assertTrue(source.contains(".uri(this.endpointUrl)"));
        assertTrue(source.contains("environment.getProperty(\"tpf.rest-client.payment.url\")"));
        assertTrue(source.contains("Missing required Spring REST client URL property 'tpf.rest-client.payment.url'"));
        assertTrue(source.contains("return normalizeBaseUrl(baseUrl) + \"/payments/\";"));
        assertFalse(source.contains("io.quarkus."));
        assertFalse(source.contains("io.smallrye.mutiny"));
        assertFalse(source.contains("jakarta.enterprise."));
        assertFalse(source.contains("jakarta.inject."));
        assertFalse(source.contains("org.eclipse.microprofile"));
        assertFalse(source.contains("@RegisterRestClient"));
        assertFalse(source.contains("@RestClient"));
        assertFalse(source.contains("PipelineInvocationRuntime"));
        assertFalse(source.contains("PipelineContextHolder"));
        assertGeneratedSourcesCompile();
    }

    @Test
    void rejectsNonUnaryRestClientStep() {
        SpringRestClientStepRenderer renderer = new SpringRestClientStepRenderer();

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> renderer.render(new RestBinding(model(StreamingShape.UNARY_STREAMING, false), "/payments"),
                new GenerationContext(null, tempDir, DeploymentRole.ORCHESTRATOR_CLIENT, Set.of(), null, null)));

        assertTrue(error.getMessage().contains("only unary-unary REST client steps"), error.getMessage());
    }

    @Test
    void rejectsSideEffectRestClientStep() {
        SpringRestClientStepRenderer renderer = new SpringRestClientStepRenderer();

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> renderer.render(new RestBinding(model(StreamingShape.UNARY_UNARY, true), "/payments"),
                new GenerationContext(null, tempDir, DeploymentRole.ORCHESTRATOR_CLIENT, Set.of(), null, null)));

        assertTrue(error.getMessage().contains("side-effect REST client steps"), error.getMessage());
    }

    @Test
    void rejectsBlankRestPathOverride() {
        SpringRestClientStepRenderer renderer = new SpringRestClientStepRenderer();

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> renderer.render(new RestBinding(model(StreamingShape.UNARY_UNARY, false), " "),
                new GenerationContext(null, tempDir, DeploymentRole.ORCHESTRATOR_CLIENT, Set.of(), null, null)));

        assertTrue(error.getMessage().contains("non-blank resource path"), error.getMessage());
    }

    private PipelineStepModel model(StreamingShape shape, boolean sideEffect) {
        return new PipelineStepModel.Builder()
            .serviceName("PaymentService")
            .generatedName("PaymentService")
            .servicePackage("com.example.service")
            .serviceClassName(ClassName.get("com.example.service", "PaymentService"))
            .inputMapping(new TypeMapping(
                ClassName.get("com.example.service", "PaymentRecord"),
                ClassName.get("com.example.service", "PaymentRecordMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("com.example.service", "PaymentStatus"),
                ClassName.get("com.example.service", "PaymentStatusMapper"),
                true))
            .streamingShape(shape)
            .enabledTargets(Set.of(GenerationTarget.REST_CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .serviceApiKind(ServiceApiKind.REACTIVE)
            .sideEffect(sideEffect)
            .build();
    }

    private void assertGeneratedSourcesCompile() throws Exception {
        writeSource("org/springframework/stereotype/Component.java",
            """
            package org.springframework.stereotype;
            public @interface Component {
            }
            """);
        writeSource("org/springframework/core/env/Environment.java",
            """
            package org.springframework.core.env;
            public interface Environment {
                String getProperty(String key);
            }
            """);
        writeSource("org/springframework/web/reactive/function/client/WebClient.java",
            """
            package org.springframework.web.reactive.function.client;
            import reactor.core.publisher.Mono;
            public class WebClient {
                public interface Builder {
                    WebClient build();
                }
                public RequestBodyUriSpec post() {
                    throw new UnsupportedOperationException("stub");
                }
                public interface RequestBodyUriSpec {
                    RequestBodySpec uri(String uri);
                }
                public interface RequestBodySpec {
                    RequestHeadersSpec bodyValue(Object body);
                }
                public interface RequestHeadersSpec {
                    ResponseSpec retrieve();
                }
                public interface ResponseSpec {
                    <T> Mono<T> bodyToMono(Class<T> elementClass);
                }
            }
            """);
        writeSource("reactor/core/publisher/Mono.java",
            """
            package reactor.core.publisher;
            import java.util.concurrent.CompletableFuture;
            import java.util.function.Function;
            public class Mono<T> {
                public static <T> Mono<T> error(Throwable throwable) {
                    throw new UnsupportedOperationException("stub");
                }
                public Mono<T> switchIfEmpty(Mono<T> alternate) {
                    throw new UnsupportedOperationException("stub");
                }
                public <R> Mono<R> map(Function<? super T, ? extends R> mapper) {
                    throw new UnsupportedOperationException("stub");
                }
                public CompletableFuture<T> toFuture() {
                    throw new UnsupportedOperationException("stub");
                }
            }
            """);
        writeSource("com/example/service/PaymentRecord.java",
            """
            package com.example.service;
            public record PaymentRecord(String value) {
            }
            """);
        writeSource("com/example/dto/PaymentRecordDto.java",
            """
            package com.example.dto;
            public record PaymentRecordDto(String value) {
            }
            """);
        writeSource("com/example/service/PaymentStatus.java",
            """
            package com.example.service;
            public record PaymentStatus(String value) {
            }
            """);
        writeSource("com/example/dto/PaymentStatusDto.java",
            """
            package com.example.dto;
            public record PaymentStatusDto(String value) {
            }
            """);

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        try (StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null)) {
            List<Path> sources;
            try (var stream = Files.walk(tempDir)) {
                sources = stream
                    .filter(path -> path.toString().endsWith(".java"))
                    .toList();
            }
            Iterable<? extends JavaFileObject> compilationUnits =
                fileManager.getJavaFileObjectsFromPaths(sources);
            Files.createDirectories(tempDir.resolve("classes"));
            Boolean compiled = compiler.getTask(
                null,
                fileManager,
                diagnostics,
                List.of(
                    "-proc:none",
                    "-classpath", System.getProperty("java.class.path"),
                    "-d", tempDir.resolve("classes").toString()),
                null,
                compilationUnits).call();
            if (!Boolean.TRUE.equals(compiled)) {
                String messages = diagnostics.getDiagnostics().stream()
                    .map(this::formatDiagnostic)
                    .toList()
                    .toString();
                throw new AssertionError("Generated Spring REST client source did not compile: " + messages);
            }
        }
    }

    private void writeSource(String relativePath, String source) throws Exception {
        Path path = tempDir.resolve(relativePath);
        Files.createDirectories(path.getParent());
        Files.writeString(path, source);
    }

    private String formatDiagnostic(Diagnostic<? extends JavaFileObject> diagnostic) {
        return diagnostic.getKind() + " " + diagnostic.getSource() + ":" + diagnostic.getLineNumber()
            + " " + diagnostic.getMessage(null);
    }
}
