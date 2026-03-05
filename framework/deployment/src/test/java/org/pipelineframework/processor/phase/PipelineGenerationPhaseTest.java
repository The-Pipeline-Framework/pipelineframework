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
    void computesEnabledAspectsFromContextModels() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        org.pipelineframework.processor.PipelineCompilationContext context =
            new org.pipelineframework.processor.PipelineCompilationContext(processingEnv, roundEnv);

        org.pipelineframework.processor.ir.PipelineAspectModel aspect1 =
            new org.pipelineframework.processor.ir.PipelineAspectModel.Builder()
                .name("Persistence")
                .scope("GLOBAL")
                .position("AFTER_STEP")
                .configuration(java.util.Map.of())
                .build();

        org.pipelineframework.processor.ir.PipelineAspectModel aspect2 =
            new org.pipelineframework.processor.ir.PipelineAspectModel.Builder()
                .name("Cache")
                .scope("GLOBAL")
                .position("BEFORE_STEP")
                .configuration(java.util.Map.of())
                .build();

        context.setAspectModels(java.util.List.of(aspect1, aspect2));

        java.lang.reflect.Method method = PipelineGenerationPhase.class.getDeclaredMethod(
            "computeEnabledAspects",
            org.pipelineframework.processor.PipelineCompilationContext.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        Set<String> enabledAspects = (Set<String>) method.invoke(phase, context);

        assertEquals(2, enabledAspects.size());
        assertTrue(enabledAspects.contains("persistence"));
        assertTrue(enabledAspects.contains("cache"));
    }

    @Test
    void computeEnabledAspectsHandlesEmptyList() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        org.pipelineframework.processor.PipelineCompilationContext context =
            new org.pipelineframework.processor.PipelineCompilationContext(processingEnv, roundEnv);

        context.setAspectModels(java.util.List.of());

        java.lang.reflect.Method method = PipelineGenerationPhase.class.getDeclaredMethod(
            "computeEnabledAspects",
            org.pipelineframework.processor.PipelineCompilationContext.class);
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        Set<String> enabledAspects = (Set<String>) method.invoke(phase, context);

        assertNotNull(enabledAspects);
        assertTrue(enabledAspects.isEmpty());
    }

    @Test
    void computeEnabledAspectsHandlesNullAspects() throws Exception {
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

        assertNotNull(enabledAspects);
        assertTrue(enabledAspects.isEmpty());
    }

    @Test
    void generatesOrchestratorArtifactsWhenFlagEnabled() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        org.pipelineframework.processor.PipelineCompilationContext context =
            new org.pipelineframework.processor.PipelineCompilationContext(processingEnv, roundEnv);
        context.setOrchestratorGenerated(true);
        context.setTransportMode(org.pipelineframework.processor.TransportMode.GRPC);

        org.pipelineframework.processor.ir.PipelineStepModel model =
            new org.pipelineframework.processor.ir.PipelineStepModel.Builder()
                .serviceName("OrchestratorService")
                .generatedName("OrchestratorService")
                .servicePackage("com.example.orchestrator.service")
                .serviceClassName(com.squareup.javapoet.ClassName.get("com.example.orchestrator.service", "OrchestratorService"))
                .inputMapping(new org.pipelineframework.processor.ir.TypeMapping(
                    com.squareup.javapoet.ClassName.get("com.example", "Input"), null, false))
                .outputMapping(new org.pipelineframework.processor.ir.TypeMapping(
                    com.squareup.javapoet.ClassName.get("com.example", "Output"), null, false))
                .streamingShape(org.pipelineframework.processor.ir.StreamingShape.UNARY_UNARY)
                .enabledTargets(java.util.Set.of(org.pipelineframework.processor.ir.GenerationTarget.GRPC_SERVICE))
                .executionMode(org.pipelineframework.processor.ir.ExecutionMode.DEFAULT)
                .deploymentRole(org.pipelineframework.processor.ir.DeploymentRole.PIPELINE_SERVER)
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
                "ProcessAlphaService",
                org.pipelineframework.processor.ir.StreamingShape.UNARY_UNARY,
                "orchestrator",
                "Orchestrator CLI",
                "1.0.0"
            );

        context.setRendererBindings(java.util.Map.of("orchestrator", binding));

        assertDoesNotThrow(() -> phase.execute(context));
    }

    @Test
    void derivesOuterClassNameWithUnderscores() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        DescriptorProtos.FileDescriptorProto fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("proto/my_custom_proto.proto")
            .build();
        Descriptors.FileDescriptor descriptor = Descriptors.FileDescriptor.buildFrom(
            fileProto,
            new Descriptors.FileDescriptor[0]);

        java.lang.reflect.Method method = PipelineGenerationPhase.class.getDeclaredMethod(
            "deriveOuterClassName",
            Descriptors.FileDescriptor.class);
        method.setAccessible(true);

        String outer = (String) method.invoke(phase, descriptor);
        assertEquals("MyCustomProto", outer);
    }

    @Test
    void derivesOuterClassNameWithDashes() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        DescriptorProtos.FileDescriptorProto fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("proto/my-service-v2.proto")
            .build();
        Descriptors.FileDescriptor descriptor = Descriptors.FileDescriptor.buildFrom(
            fileProto,
            new Descriptors.FileDescriptor[0]);

        java.lang.reflect.Method method = PipelineGenerationPhase.class.getDeclaredMethod(
            "deriveOuterClassName",
            Descriptors.FileDescriptor.class);
        method.setAccessible(true);

        String outer = (String) method.invoke(phase, descriptor);
        assertEquals("MyServiceV2", outer);
    }

    @Test
    void resolvesRoleOutputDirForPipelineServer() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        org.pipelineframework.processor.PipelineCompilationContext context =
            new org.pipelineframework.processor.PipelineCompilationContext(processingEnv, roundEnv);
        Path rootPath = Path.of("target/generated-sources");
        context.setGeneratedSourcesRoot(rootPath);

        java.lang.reflect.Method method = PipelineGenerationPhase.class.getDeclaredMethod(
            "resolveRoleOutputDir",
            org.pipelineframework.processor.PipelineCompilationContext.class,
            org.pipelineframework.processor.ir.DeploymentRole.class);
        method.setAccessible(true);

        Path resolved = (Path) method.invoke(phase, context, org.pipelineframework.processor.ir.DeploymentRole.PIPELINE_SERVER);

        assertNotNull(resolved);
        assertTrue(resolved.toString().contains("pipeline-server"));
    }

    @Test
    void handlesNullProcessingEnvironmentGracefully() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        org.pipelineframework.processor.PipelineCompilationContext context =
            new org.pipelineframework.processor.PipelineCompilationContext(null, roundEnv);

        assertDoesNotThrow(() -> phase.execute(context));
    }
}