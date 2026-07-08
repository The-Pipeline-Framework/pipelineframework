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

package org.pipelineframework.processor.phase;

import java.util.List;
import java.util.Set;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.PlatformMode;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.processor.ir.ServiceApiKind;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpringRendererProfileSupportTest {

    @Test
    void acceptsLocalComputeUnaryStep() {
        PipelineCompilationContext context = context();

        assertDoesNotThrow(() -> SpringRendererProfileSupport.validateGenerationSupported(context));
    }

    @Test
    void acceptsRestComputeUnaryStep() {
        PipelineCompilationContext context = context();
        context.setTransportMode(PipelineTransport.REST);
        context.setStepModels(List.of(step(
            Set.of(GenerationTarget.REST_RESOURCE, GenerationTarget.LOCAL_CLIENT_STEP),
            StreamingShape.UNARY_UNARY,
            ServiceApiKind.REACTIVE)));

        assertDoesNotThrow(() -> SpringRendererProfileSupport.validateGenerationSupported(context));
    }

    @Test
    void acceptsUnaryRestClientStep() {
        PipelineCompilationContext context = context();
        context.setTransportMode(PipelineTransport.REST);
        context.setStepModels(List.of(step(
            Set.of(GenerationTarget.REST_CLIENT_STEP),
            StreamingShape.UNARY_UNARY,
            ServiceApiKind.REACTIVE,
            DeploymentRole.ORCHESTRATOR_CLIENT)));

        assertDoesNotThrow(() -> SpringRendererProfileSupport.validateGenerationSupported(context));
    }

    @Test
    void acceptsBlockingUnaryStep() {
        PipelineCompilationContext context = context();
        context.setStepModels(List.of(step(Set.of(GenerationTarget.LOCAL_CLIENT_STEP), StreamingShape.UNARY_UNARY,
            ServiceApiKind.BLOCKING)));

        assertDoesNotThrow(() -> SpringRendererProfileSupport.validateGenerationSupported(context));
    }

    @Test
    void acceptsUnaryDelegatedSpringBeanStep() {
        PipelineCompilationContext context = context();
        context.setStepModels(List.of(delegatedStep(
            Set.of(GenerationTarget.LOCAL_CLIENT_STEP),
            StreamingShape.UNARY_UNARY,
            ServiceApiKind.REACTIVE)));

        assertDoesNotThrow(() -> SpringRendererProfileSupport.validateGenerationSupported(context));
    }

    @Test
    void rejectsGrpcTransport() {
        PipelineCompilationContext context = context();
        context.setTransportMode(PipelineTransport.GRPC);

        assertThrows(IllegalStateException.class,
            () -> SpringRendererProfileSupport.validateGenerationSupported(context));
    }

    @Test
    void rejectsFunctionPlatform() {
        PipelineCompilationContext context = context();
        context.setPlatformMode(PlatformMode.FUNCTION);

        assertThrows(IllegalStateException.class,
            () -> SpringRendererProfileSupport.validateGenerationSupported(context));
    }

    @Test
    void rejectsUnsupportedTargets() {
        PipelineCompilationContext context = context();
        context.setStepModels(List.of(step(Set.of(GenerationTarget.REST_RESOURCE), StreamingShape.UNARY_UNARY,
            ServiceApiKind.REACTIVE)));

        assertThrows(IllegalStateException.class,
            () -> SpringRendererProfileSupport.validateGenerationSupported(context));
    }

    @Test
    void rejectsRestClientStepForPipelineServerRole() {
        PipelineCompilationContext context = context();
        context.setTransportMode(PipelineTransport.REST);
        context.setStepModels(List.of(step(Set.of(GenerationTarget.REST_CLIENT_STEP), StreamingShape.UNARY_UNARY,
            ServiceApiKind.REACTIVE, DeploymentRole.PIPELINE_SERVER)));

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> SpringRendererProfileSupport.validateGenerationSupported(context));
        assertTrue(error.getMessage().contains("REST client steps only for client-role boundaries"),
            error.getMessage());
    }

    @Test
    void rejectsRestClientStepForRestServerRole() {
        PipelineCompilationContext context = context();
        context.setTransportMode(PipelineTransport.REST);
        context.setStepModels(List.of(step(Set.of(GenerationTarget.REST_CLIENT_STEP), StreamingShape.UNARY_UNARY,
            ServiceApiKind.REACTIVE, DeploymentRole.REST_SERVER)));

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> SpringRendererProfileSupport.validateGenerationSupported(context));
        assertTrue(error.getMessage().contains("REST client steps only for client-role boundaries"),
            error.getMessage());
    }

    @Test
    void rejectsRestClientStepForPluginServerRole() {
        PipelineCompilationContext context = context();
        context.setTransportMode(PipelineTransport.REST);
        context.setStepModels(List.of(step(Set.of(GenerationTarget.REST_CLIENT_STEP), StreamingShape.UNARY_UNARY,
            ServiceApiKind.REACTIVE, DeploymentRole.PLUGIN_SERVER)));

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> SpringRendererProfileSupport.validateGenerationSupported(context));
        assertTrue(error.getMessage().contains("REST client steps only for client-role boundaries"),
            error.getMessage());
    }

    @Test
    void rejectsBlockingIteratorStep() {
        PipelineCompilationContext context = context();
        context.setStepModels(List.of(step(Set.of(GenerationTarget.LOCAL_CLIENT_STEP), StreamingShape.UNARY_UNARY,
            ServiceApiKind.BLOCKING_ITERATOR)));

        assertThrows(IllegalStateException.class,
            () -> SpringRendererProfileSupport.validateGenerationSupported(context));
    }

    @Test
    void rejectsDelegatedNonUnaryStep() {
        PipelineCompilationContext context = context();
        context.setStepModels(List.of(delegatedStep(
            Set.of(GenerationTarget.LOCAL_CLIENT_STEP),
            StreamingShape.UNARY_STREAMING,
            ServiceApiKind.REACTIVE)));

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> SpringRendererProfileSupport.validateGenerationSupported(context));
        assertTrue(error.getMessage().contains("supports only unary-unary steps"), error.getMessage());
    }

    @Test
    void rejectsDelegatedRestResourceTarget() {
        PipelineCompilationContext context = context();
        context.setTransportMode(PipelineTransport.REST);
        context.setStepModels(List.of(delegatedStep(
            Set.of(GenerationTarget.REST_RESOURCE, GenerationTarget.LOCAL_CLIENT_STEP),
            StreamingShape.UNARY_UNARY,
            ServiceApiKind.REACTIVE)));

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> SpringRendererProfileSupport.validateGenerationSupported(context));
        assertTrue(error.getMessage().contains("delegated Spring beans only as unary local client steps"),
            error.getMessage());
    }

    private PipelineCompilationContext context() {
        PipelineCompilationContext context = new PipelineCompilationContext(null, null);
        context.setRendererProfile("spring");
        context.setTransportMode(PipelineTransport.LOCAL);
        context.setPlatformMode(PlatformMode.COMPUTE);
        context.setStepModels(List.of(step(Set.of(GenerationTarget.LOCAL_CLIENT_STEP), StreamingShape.UNARY_UNARY,
            ServiceApiKind.REACTIVE)));
        return context;
    }

    private PipelineStepModel step(
        Set<GenerationTarget> targets,
        StreamingShape shape,
        ServiceApiKind apiKind
    ) {
        return new PipelineStepModel.Builder()
            .serviceName("PaymentService")
            .generatedName("PaymentService")
            .servicePackage("com.example.service")
            .serviceClassName(ClassName.get("com.example.service", "PaymentService"))
            .inputMapping(new TypeMapping(ClassName.get("com.example.service", "PaymentRecord"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.service", "PaymentStatus"), null, false))
            .streamingShape(shape)
            .enabledTargets(targets)
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .serviceApiKind(apiKind)
            .build();
    }

    private PipelineStepModel step(
        Set<GenerationTarget> targets,
        StreamingShape shape,
        ServiceApiKind apiKind,
        DeploymentRole role
    ) {
        return step(targets, shape, apiKind).toBuilder()
            .deploymentRole(role)
            .build();
    }

    private PipelineStepModel delegatedStep(
        Set<GenerationTarget> targets,
        StreamingShape shape,
        ServiceApiKind apiKind
    ) {
        return new PipelineStepModel.Builder()
            .serviceName("AuditService")
            .generatedName("AuditService")
            .servicePackage("com.example.service")
            .serviceClassName(ClassName.get("com.example.service", "AuditService"))
            .inputMapping(new TypeMapping(ClassName.get("com.example.service", "PaymentStatus"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.service", "PaymentStatus"), null, false))
            .streamingShape(shape)
            .enabledTargets(targets)
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .delegateService(ClassName.get("com.example.service", "PaymentAuditBean"))
            .serviceApiKind(apiKind)
            .build();
    }
}
