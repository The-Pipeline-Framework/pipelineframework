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

    @Test
    void computesEnabledAspectsFromContext() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        org.pipelineframework.processor.PipelineCompilationContext context =
            new org.pipelineframework.processor.PipelineCompilationContext(processingEnv, roundEnv);

        org.pipelineframework.processor.ir.PipelineAspectModel aspect1 =
            new org.pipelineframework.processor.ir.PipelineAspectModel("Cache", "AFTER_STEP", java.util.Map.of());
        org.pipelineframework.processor.ir.PipelineAspectModel aspect2 =
            new org.pipelineframework.processor.ir.PipelineAspectModel("Persistence", "AFTER_STEP", java.util.Map.of());

        context.setAspectModels(java.util.List.of(aspect1, aspect2));

        java.lang.reflect.Method method = PipelineGenerationPhase.class.getDeclaredMethod(
            "computeEnabledAspects",
            org.pipelineframework.processor.PipelineCompilationContext.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        Set<String> enabledAspects = (Set<String>) method.invoke(phase, context);

        assertEquals(Set.of("cache", "persistence"), enabledAspects);
    }

    @Test
    void handlesNullAspectModelsWhenComputingEnabledAspects() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        org.pipelineframework.processor.PipelineCompilationContext context =
            new org.pipelineframework.processor.PipelineCompilationContext(processingEnv, roundEnv);
        context.setAspectModels(null);

        java.lang.reflect.Method method = PipelineGenerationPhase.class.getDeclaredMethod(
            "computeEnabledAspects",
            org.pipelineframework.processor.PipelineCompilationContext.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        Set<String> enabledAspects = (Set<String>) method.invoke(phase, context);

        assertTrue(enabledAspects.isEmpty());
    }

    @Test
    void resolvesCacheKeyGeneratorFromOptions() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        when(processingEnv.getOptions()).thenReturn(java.util.Map.of(
            "pipeline.cache.keyGenerator", "com.example.CustomKeyGenerator"
        ));

        java.lang.reflect.Method method = PipelineGenerationPhase.class.getDeclaredMethod(
            "resolveCacheKeyGenerator",
            org.pipelineframework.processor.PipelineCompilationContext.class);
        method.setAccessible(true);

        org.pipelineframework.processor.PipelineCompilationContext context =
            new org.pipelineframework.processor.PipelineCompilationContext(processingEnv, roundEnv);

        com.squareup.javapoet.ClassName keyGenerator =
            (com.squareup.javapoet.ClassName) method.invoke(phase, context);

        assertNotNull(keyGenerator);
        assertEquals("CustomKeyGenerator", keyGenerator.simpleName());
        assertEquals("com.example", keyGenerator.packageName());
    }

    @Test
    void returnsNullWhenCacheKeyGeneratorNotConfigured() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        when(processingEnv.getOptions()).thenReturn(java.util.Map.of());

        java.lang.reflect.Method method = PipelineGenerationPhase.class.getDeclaredMethod(
            "resolveCacheKeyGenerator",
            org.pipelineframework.processor.PipelineCompilationContext.class);
        method.setAccessible(true);

        org.pipelineframework.processor.PipelineCompilationContext context =
            new org.pipelineframework.processor.PipelineCompilationContext(processingEnv, roundEnv);

        com.squareup.javapoet.ClassName keyGenerator =
            (com.squareup.javapoet.ClassName) method.invoke(phase, context);

        assertNull(keyGenerator);
    }

    @Test
    void generatesOrchestratorArtifactsWhenEnabled() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        org.pipelineframework.processor.PipelineCompilationContext context =
            new org.pipelineframework.processor.PipelineCompilationContext(processingEnv, roundEnv);
        context.setOrchestratorGenerated(true);

        org.pipelineframework.processor.ir.PipelineStepModel model =
            new org.pipelineframework.processor.ir.PipelineStepModel.Builder()
                .serviceName("OrchestratorService")
                .generatedName("OrchestratorService")
                .servicePackage("com.example.orchestrator.service")
                .serviceClassName(com.squareup.javapoet.ClassName.get("com.example.orchestrator.service", "OrchestratorService"))
                .inputMapping(new org.pipelineframework.processor.ir.TypeMapping(
                    com.squareup.javapoet.ClassName.get("com.example", "InputDto"), null, false))
                .outputMapping(new org.pipelineframework.processor.ir.TypeMapping(
                    com.squareup.javapoet.ClassName.get("com.example", "OutputDto"), null, false))
                .streamingShape(org.pipelineframework.processor.ir.StreamingShape.UNARY_UNARY)
                .enabledTargets(java.util.Set.of(org.pipelineframework.processor.ir.GenerationTarget.GRPC_SERVICE))
                .executionMode(org.pipelineframework.processor.ir.ExecutionMode.DEFAULT)
                .deploymentRole(org.pipelineframework.processor.ir.DeploymentRole.ORCHESTRATOR_CLIENT)
                .sideEffect(false)
                .cacheKeyGenerator(null)
                .orderingRequirement(org.pipelineframework.parallelism.OrderingRequirement.RELAXED)
                .threadSafety(org.pipelineframework.parallelism.ThreadSafety.SAFE)
                .build();

        org.pipelineframework.processor.ir.OrchestratorBinding binding =
            new org.pipelineframework.processor.ir.OrchestratorBinding(
                model,
                "com.example",
                "GRPC",
                "Input",
                "Output",
                false,
                false,
                "ProcessFirstService",
                org.pipelineframework.processor.ir.StreamingShape.UNARY_UNARY,
                null,
                null,
                null
            );

        context.setRendererBindings(java.util.Map.of("orchestrator", binding));
        context.setStepModels(java.util.List.of(model));

        assertDoesNotThrow(() -> phase.execute(context));
    }

    @Test
    void derivesOuterClassNameWithCustomJavaOuterClassname() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        DescriptorProtos.FileDescriptorProto fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("service.proto")
            .setOptions(DescriptorProtos.FileOptions.newBuilder()
                .setJavaOuterClassname("CustomOuterClass")
                .build())
            .build();
        Descriptors.FileDescriptor descriptor = Descriptors.FileDescriptor.buildFrom(
            fileProto,
            new Descriptors.FileDescriptor[0]);

        java.lang.reflect.Method method = PipelineGenerationPhase.class.getDeclaredMethod(
            "deriveOuterClassName",
            Descriptors.FileDescriptor.class);
        method.setAccessible(true);

        String outer = (String) method.invoke(phase, descriptor);
        assertEquals("CustomOuterClass", outer);
    }

    @Test
    void derivesOuterClassNameHandlingComplexFilePaths() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        DescriptorProtos.FileDescriptorProto fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("path/to/deeply/nested-file_name.proto")
            .build();
        Descriptors.FileDescriptor descriptor = Descriptors.FileDescriptor.buildFrom(
            fileProto,
            new Descriptors.FileDescriptor[0]);

        java.lang.reflect.Method method = PipelineGenerationPhase.class.getDeclaredMethod(
            "deriveOuterClassName",
            Descriptors.FileDescriptor.class);
        method.setAccessible(true);

        String outer = (String) method.invoke(phase, descriptor);
        assertEquals("NestedFileName", outer);
    }

    @Test
    void skipsSideEffectBeanGenerationWhenAlreadyGenerated() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        org.pipelineframework.processor.PipelineCompilationContext context =
            new org.pipelineframework.processor.PipelineCompilationContext(processingEnv, roundEnv);

        org.pipelineframework.processor.ir.PipelineStepModel model =
            new org.pipelineframework.processor.ir.PipelineStepModel.Builder()
                .serviceName("ProcessSideEffectService")
                .generatedName("ProcessSideEffectService")
                .servicePackage("com.example")
                .serviceClassName(com.squareup.javapoet.ClassName.get("com.example", "SideEffectService"))
                .inputMapping(new org.pipelineframework.processor.ir.TypeMapping(
                    com.squareup.javapoet.ClassName.get("com.example", "Input"), null, false))
                .outputMapping(new org.pipelineframework.processor.ir.TypeMapping(
                    com.squareup.javapoet.ClassName.get("com.example", "Output"), null, false))
                .streamingShape(org.pipelineframework.processor.ir.StreamingShape.UNARY_UNARY)
                .enabledTargets(java.util.Set.of(org.pipelineframework.processor.ir.GenerationTarget.GRPC_SERVICE))
                .executionMode(org.pipelineframework.processor.ir.ExecutionMode.DEFAULT)
                .deploymentRole(org.pipelineframework.processor.ir.DeploymentRole.PIPELINE_SERVER)
                .sideEffect(true)
                .cacheKeyGenerator(null)
                .orderingRequirement(org.pipelineframework.parallelism.OrderingRequirement.RELAXED)
                .threadSafety(org.pipelineframework.parallelism.ThreadSafety.SAFE)
                .build();

        context.setStepModels(java.util.List.of(model));
        context.setRendererBindings(java.util.Map.of());

        assertDoesNotThrow(() -> phase.execute(context));
    }
}