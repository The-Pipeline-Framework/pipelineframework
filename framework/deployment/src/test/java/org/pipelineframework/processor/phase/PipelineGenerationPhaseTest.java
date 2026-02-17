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

package org.pipelineframework.processor.phase;

import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import java.nio.file.Path;
import java.util.Set;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit tests for PipelineGenerationPhase */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PipelineGenerationPhaseTest {

    @Mock
    private ProcessingEnvironment processingEnv;

    @Mock
    private RoundEnvironment roundEnv;

    @Mock
    private Messager messager;

    @BeforeEach
    void setUp() {
        when(processingEnv.getMessager()).thenReturn(messager);
        when(processingEnv.getElementUtils())
                .thenReturn(mock(javax.lang.model.util.Elements.class));
        javax.annotation.processing.Filer filer = mock(javax.annotation.processing.Filer.class);
        FileObject fileObject = mock(FileObject.class);
        try {
            when(fileObject.openWriter()).thenReturn(new java.io.StringWriter());
            when(filer.createResource(
                any(StandardLocation.class), anyString(), anyString(), any(javax.lang.model.element.Element[].class)))
                .thenReturn(fileObject);
            when(filer.createResource(any(StandardLocation.class), anyString(), anyString()))
                .thenReturn(fileObject);
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
        when(processingEnv.getFiler()).thenReturn(filer);
        when(processingEnv.getSourceVersion()).thenReturn(SourceVersion.RELEASE_21);
    }

    @Test
    void testGenerationPhaseInitialization() {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        assertNotNull(phase);
        assertEquals("Pipeline Generation Phase", phase.name());
    }

    @Test
    void testGenerationPhaseExecutionHandlesEmptyContextGracefully() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        org.pipelineframework.processor.PipelineCompilationContext context =
            new org.pipelineframework.processor.PipelineCompilationContext(processingEnv, roundEnv);

        // Execute the phase with an empty context (no models)
        // This should not throw exceptions and should handle the empty case
        assertDoesNotThrow(() -> phase.execute(context));
    }

    @Test
    void derivesOuterClassNameFromProtoPath() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        DescriptorProtos.FileDescriptorProto fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("proto/search/baz.proto")
            .build();
        Descriptors.FileDescriptor descriptor = Descriptors.FileDescriptor.buildFrom(
            fileProto,
            new Descriptors.FileDescriptor[0]);

        java.lang.reflect.Method method = PipelineGenerationPhase.class.getDeclaredMethod(
            "deriveOuterClassName",
            Descriptors.FileDescriptor.class);
        method.setAccessible(true);

        String outer = (String) method.invoke(phase, descriptor);
        assertEquals("Baz", outer);
    }

    @Test
    void resolveClientRoleDefaultsToOrchestratorClientWhenNull() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        java.lang.reflect.Method method = PipelineGenerationPhase.class.getDeclaredMethod(
            "resolveClientRole",
            org.pipelineframework.processor.ir.DeploymentRole.class);
        method.setAccessible(true);

        Object role = method.invoke(phase, new Object[]{null});
        assertEquals(org.pipelineframework.processor.ir.DeploymentRole.ORCHESTRATOR_CLIENT, role);
    }

    @Test
    void skipsClientStepGenerationWhenGrpcBindingMissing() {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        org.pipelineframework.processor.PipelineCompilationContext context =
            new org.pipelineframework.processor.PipelineCompilationContext(processingEnv, roundEnv);

        org.pipelineframework.processor.ir.PipelineStepModel model =
            new org.pipelineframework.processor.ir.PipelineStepModel.Builder()
                .serviceName("ProcessMissingBindingService")
                .generatedName("ProcessMissingBindingService")
                .servicePackage("com.example")
                .serviceClassName(com.squareup.javapoet.ClassName.get("com.example", "MissingBindingService"))
                .inputMapping(new org.pipelineframework.processor.ir.TypeMapping(
                    com.squareup.javapoet.ClassName.get("com.example", "In"), null, false))
                .outputMapping(new org.pipelineframework.processor.ir.TypeMapping(
                    com.squareup.javapoet.ClassName.get("com.example", "Out"), null, false))
                .streamingShape(org.pipelineframework.processor.ir.StreamingShape.UNARY_UNARY)
                .enabledTargets(java.util.Set.of(org.pipelineframework.processor.ir.GenerationTarget.CLIENT_STEP))
                .executionMode(org.pipelineframework.processor.ir.ExecutionMode.DEFAULT)
                .deploymentRole(org.pipelineframework.processor.ir.DeploymentRole.ORCHESTRATOR_CLIENT)
                .sideEffect(false)
                .cacheKeyGenerator(null)
                .orderingRequirement(org.pipelineframework.parallelism.OrderingRequirement.RELAXED)
                .threadSafety(org.pipelineframework.parallelism.ThreadSafety.SAFE)
                .build();

        context.setStepModels(java.util.List.of(model));
        context.setRendererBindings(java.util.Map.of());

        assertDoesNotThrow(() -> phase.execute(context));
    }

    @Test
    void externalAdapterGenerationContextPropagatesEnabledAspects() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        org.pipelineframework.processor.PipelineCompilationContext context =
            new org.pipelineframework.processor.PipelineCompilationContext(processingEnv, roundEnv);
        context.setGeneratedSourcesRoot(Path.of("target/generated-sources-test"));

        java.lang.reflect.Method method = PipelineGenerationPhase.class.getDeclaredMethod(
            "createExternalAdapterGenerationContext",
            org.pipelineframework.processor.PipelineCompilationContext.class,
            org.pipelineframework.processor.ir.DeploymentRole.class,
            Set.class,
            com.squareup.javapoet.ClassName.class,
            com.google.protobuf.DescriptorProtos.FileDescriptorSet.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        org.pipelineframework.processor.renderer.GenerationContext generationContext =
            (org.pipelineframework.processor.renderer.GenerationContext) method.invoke(
                phase,
                context,
                org.pipelineframework.processor.ir.DeploymentRole.PIPELINE_SERVER,
                Set.of("cache", "audit"),
                null,
                null);

        assertEquals(Set.of("cache", "audit"), generationContext.enabledAspects());
        assertEquals(org.pipelineframework.processor.ir.DeploymentRole.PIPELINE_SERVER, generationContext.role());
    }
}
