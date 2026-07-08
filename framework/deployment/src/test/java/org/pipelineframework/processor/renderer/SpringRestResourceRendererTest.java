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
import java.util.Set;

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

class SpringRestResourceRendererTest {

    @TempDir
    Path tempDir;

    @Test
    void rendersSpringNativeUnaryRestResource() throws Exception {
        SpringRestResourceRenderer renderer = new SpringRestResourceRenderer();

        renderer.render(new RestBinding(model(StreamingShape.UNARY_UNARY, ServiceApiKind.REACTIVE), "/payments"),
            new GenerationContext(null, tempDir, DeploymentRole.REST_SERVER, Set.of(), null, null));

        Path resource = tempDir.resolve("com/example/service/pipeline/PaymentResource.java");
        String source = Files.readString(resource);
        assertTrue(source.contains("import org.springframework.web.bind.annotation.PostMapping;"));
        assertTrue(source.contains("import org.springframework.web.bind.annotation.RequestMapping;"));
        assertTrue(source.contains("import org.springframework.web.bind.annotation.RequestBody;"));
        assertTrue(source.contains("import org.springframework.web.bind.annotation.RequestHeader;"));
        assertTrue(source.contains("import org.springframework.web.bind.annotation.RestController;"));
        assertTrue(source.contains("import org.pipelineframework.runtime.core.TpfContextHeaders;"));
        assertTrue(source.contains("import org.pipelineframework.runtime.core.TpfExecutionContext;"));
        assertTrue(source.contains("import reactor.core.publisher.Mono;"));
        assertTrue(source.contains("private final SpringPipelineRunner pipelineRunner;"));
        assertTrue(source.contains("public PaymentResource(SpringPipelineRunner pipelineRunner,"));
        assertTrue(source.contains("public Mono<PaymentStatusDto> process(@RequestBody PaymentRecordDto inputDto,"));
        assertTrue(source.contains("@RequestHeader(name = TpfContextHeaders.VERSION, required = false) String tpfVersionTag"));
        assertTrue(source.contains("@RequestHeader(name = TpfContextHeaders.REPLAY, required = false) String tpfReplayMode"));
        assertTrue(source.contains("@RequestHeader(name = TpfContextHeaders.CACHE_POLICY, required = false) String tpfCachePolicy"));
        assertTrue(source.contains("return Mono.defer(() -> {"));
        assertTrue(source.contains("try (TpfExecutionContext.Scope tpfContextScope = TpfExecutionContext.withHeaders(tpfVersionTag, tpfReplayMode, tpfCachePolicy))"));
        assertTrue(source.contains("PaymentRecord inputDomain = this.inboundMapper.fromExternal(inputDto);"));
        assertTrue(source.contains("return Mono.fromCompletionStage(this.pipelineRunner.run(inputDomain))"));
        assertTrue(source.contains(".map(output -> this.outboundMapper.toExternal((PaymentStatus) output))"));
        assertFalse(source.contains(".doFinally(signalType -> tpfContextScope.close())"));
        assertFalse(source.contains("catch (RuntimeException | Error failure)"));
        assertFalse(source.contains("io.quarkus."));
        assertFalse(source.contains("jakarta.enterprise."));
        assertFalse(source.contains("jakarta.inject."));
        assertFalse(source.contains("io.vertx."));
        assertFalse(source.contains("io.smallrye.mutiny"));
        assertFalse(source.contains("org.jboss.resteasy."));
        assertFalse(source.contains("jakarta.ws.rs."));
    }

    @Test
    void resolvesDefaultResourcePathWhenOverrideIsAbsent() throws Exception {
        SpringRestResourceRenderer renderer = new SpringRestResourceRenderer();

        renderer.render(new RestBinding(model(StreamingShape.UNARY_UNARY, ServiceApiKind.REACTIVE), null),
            new GenerationContext(null, tempDir, DeploymentRole.REST_SERVER, Set.of(), null, null));

        Path resource = tempDir.resolve("com/example/service/pipeline/PaymentResource.java");
        String source = Files.readString(resource);
        assertTrue(source.contains("@RequestMapping(\"/api/v1/payment-status\")"));
    }

    @Test
    void rejectsNonUnaryShape() {
        SpringRestResourceRenderer renderer = new SpringRestResourceRenderer();

        assertThrows(IllegalArgumentException.class,
            () -> renderer.render(new RestBinding(model(StreamingShape.UNARY_STREAMING, ServiceApiKind.REACTIVE), null),
                new GenerationContext(null, tempDir, DeploymentRole.REST_SERVER, Set.of(), null, null)));
    }

    @Test
    void rendersBlockingAuthoredRestResource() throws Exception {
        SpringRestResourceRenderer renderer = new SpringRestResourceRenderer();

        renderer.render(new RestBinding(model(StreamingShape.UNARY_UNARY, ServiceApiKind.BLOCKING), "/payments"),
            new GenerationContext(null, tempDir, DeploymentRole.REST_SERVER, Set.of(), null, null));

        Path resource = tempDir.resolve("com/example/service/pipeline/PaymentResource.java");
        String source = Files.readString(resource);
        assertTrue(source.contains("private final SpringPipelineRunner pipelineRunner;"));
        assertTrue(source.contains("return Mono.fromCompletionStage(this.pipelineRunner.run(inputDomain))"));
    }

    @Test
    void blockingRestResourceContainsNoQuarkusOrMutinyImports() throws Exception {
        SpringRestResourceRenderer renderer = new SpringRestResourceRenderer();

        renderer.render(new RestBinding(model(StreamingShape.UNARY_UNARY, ServiceApiKind.BLOCKING), null),
            new GenerationContext(null, tempDir, DeploymentRole.REST_SERVER, Set.of(), null, null));

        Path resource = tempDir.resolve("com/example/service/pipeline/PaymentResource.java");
        String source = Files.readString(resource);
        assertFalse(source.contains("io.quarkus."), "Blocking REST resource must not contain Quarkus imports");
        assertFalse(source.contains("jakarta.enterprise."), "Blocking REST resource must not contain CDI imports");
        assertFalse(source.contains("io.vertx."), "Blocking REST resource must not contain Vert.x imports");
        assertFalse(source.contains("io.smallrye.mutiny"), "Blocking REST resource must not contain Mutiny imports");
        assertFalse(source.contains("org.jboss.resteasy."), "Blocking REST resource must not contain RESTEasy imports");
        assertFalse(source.contains("jakarta.ws.rs."), "Blocking REST resource must not contain JAX-RS imports");
    }

    @Test
    void blockingRestResourceWithVirtualThreadsModeBehavesSameAsDefault() throws Exception {
        SpringRestResourceRenderer renderer = new SpringRestResourceRenderer();

        // With virtual threads execution mode - REST resource delegates to SpringPipelineRunner,
        // not to a blocking call directly, so the REST resource output should be the same.
        PipelineStepModel virtualThreadsModel = modelBuilder(StreamingShape.UNARY_UNARY, ServiceApiKind.BLOCKING)
            .executionMode(ExecutionMode.VIRTUAL_THREADS)
            .build();

        renderer.render(new RestBinding(virtualThreadsModel, "/payments"),
            new GenerationContext(null, tempDir, DeploymentRole.REST_SERVER, Set.of(), null, null));

        Path resource = tempDir.resolve("com/example/service/pipeline/PaymentResource.java");
        String source = Files.readString(resource);
        assertTrue(source.contains("return Mono.fromCompletionStage(this.pipelineRunner.run(inputDomain))"),
            "REST resource must still route through SpringPipelineRunner for virtual-threads blocking steps");
        assertFalse(source.contains("RuntimeAdapters"),
            "REST resource must not call RuntimeAdapters directly; that is the local client step's job");
    }

    @Test
    void rejectsBlockingIteratorResource() {
        SpringRestResourceRenderer renderer = new SpringRestResourceRenderer();

        assertThrows(IllegalArgumentException.class,
            () -> renderer.render(new RestBinding(model(StreamingShape.UNARY_UNARY, ServiceApiKind.BLOCKING_ITERATOR), null),
                new GenerationContext(null, tempDir, DeploymentRole.REST_SERVER, Set.of(), null, null)));
    }

    @Test
    void rejectsMissingUnaryDomainTypes() {
        SpringRestResourceRenderer renderer = new SpringRestResourceRenderer();

        PipelineStepModel missingInput = modelBuilder(StreamingShape.UNARY_UNARY, ServiceApiKind.REACTIVE)
            .inputMapping(new TypeMapping(null, null, false))
            .build();

        assertThrows(IllegalArgumentException.class,
            () -> renderer.render(new RestBinding(missingInput, null),
                new GenerationContext(null, tempDir, DeploymentRole.REST_SERVER, Set.of(), null, null)));
    }

    @Test
    void rejectsMissingUnaryOutputDomainType() {
        SpringRestResourceRenderer renderer = new SpringRestResourceRenderer();

        PipelineStepModel missingOutput = modelBuilder(StreamingShape.UNARY_UNARY, ServiceApiKind.REACTIVE)
            .outputMapping(new TypeMapping(null, null, false))
            .build();

        assertThrows(IllegalArgumentException.class,
            () -> renderer.render(new RestBinding(missingOutput, null),
                new GenerationContext(null, tempDir, DeploymentRole.REST_SERVER, Set.of(), null, null)));
    }

    @Test
    void rejectsSideEffectResource() {
        SpringRestResourceRenderer renderer = new SpringRestResourceRenderer();

        PipelineStepModel sideEffect = modelBuilder(StreamingShape.UNARY_UNARY, ServiceApiKind.REACTIVE)
            .sideEffect(true)
            .build();

        assertThrows(IllegalArgumentException.class,
            () -> renderer.render(new RestBinding(sideEffect, null),
                new GenerationContext(null, tempDir, DeploymentRole.REST_SERVER, Set.of(), null, null)));
    }

    @Test
    void rejectsDelegatedResource() {
        SpringRestResourceRenderer renderer = new SpringRestResourceRenderer();

        PipelineStepModel delegated = modelBuilder(StreamingShape.UNARY_UNARY, ServiceApiKind.REACTIVE)
            .delegateService(ClassName.get("com.example.service", "DelegateService"))
            .build();

        assertThrows(IllegalArgumentException.class,
            () -> renderer.render(new RestBinding(delegated, null),
                new GenerationContext(null, tempDir, DeploymentRole.REST_SERVER, Set.of(), null, null)));
    }

    private PipelineStepModel model(StreamingShape shape, ServiceApiKind apiKind) {
        return modelBuilder(shape, apiKind).build();
    }

    private PipelineStepModel.Builder modelBuilder(StreamingShape shape, ServiceApiKind apiKind) {
        return new PipelineStepModel.Builder()
            .serviceName("PaymentService")
            .generatedName("PaymentService")
            .servicePackage("com.example.service")
            .serviceClassName(ClassName.get("com.example.service", "PaymentService"))
            .inputMapping(new TypeMapping(ClassName.get("com.example.service.domain", "PaymentRecord"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.service.domain", "PaymentStatus"), null, false))
            .streamingShape(shape)
            .enabledTargets(Set.of(GenerationTarget.REST_RESOURCE, GenerationTarget.LOCAL_CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .serviceApiKind(apiKind);
    }
}
