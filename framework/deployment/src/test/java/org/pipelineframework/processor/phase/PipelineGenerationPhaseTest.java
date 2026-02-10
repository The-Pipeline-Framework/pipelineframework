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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;
import javax.lang.model.SourceVersion;
import javax.tools.Diagnostic;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.AspectPosition;
import org.pipelineframework.processor.ir.AspectScope;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.LocalBinding;
import org.pipelineframework.processor.ir.PipelineAspectModel;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TransportMode;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.processor.mapping.PipelineRuntimeMapping;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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

    @Mock
    private Elements elements;

    @BeforeEach
    void setUp() {
        when(processingEnv.getMessager()).thenReturn(messager);
        when(processingEnv.getElementUtils()).thenReturn(elements);
        when(processingEnv.getFiler()).thenReturn(mock(javax.annotation.processing.Filer.class));
        when(processingEnv.getSourceVersion()).thenReturn(SourceVersion.RELEASE_21);
        when(processingEnv.getOptions()).thenReturn(Map.of());
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
    void testGenerationPhaseExecutionWithModels() throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);

        // Execute the phase with an empty context (no models)
        // This should not throw exceptions and should handle the empty case gracefully
        assertDoesNotThrow(() -> phase.execute(context));
    }

    @Test
    void generatesGrpcServiceForGrpcTransport(@TempDir Path tempDir) throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        GrpcDescriptors grpcDescriptors = createGrpcDescriptors();
        PipelineStepModel model = buildStepModel(
            Set.of(GenerationTarget.GRPC_SERVICE),
            DeploymentRole.PIPELINE_SERVER,
            false,
            false
        );

        PipelineCompilationContext ctx = newContext(tempDir, List.of(model), TransportMode.GRPC);
        ctx.setRendererBindings(Map.of(
            model.serviceName() + "_grpc", new GrpcBinding(model, grpcDescriptors.service(), grpcDescriptors.method())
        ));

        phase.execute(ctx);

        assertTrue(Files.exists(tempDir.resolve(
            "pipeline-server/com/example/service/pipeline/TestServiceGrpcService.java")));
    }

    @Test
    void skipsGrpcServiceGenerationForLocalTransport(@TempDir Path tempDir) throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        GrpcDescriptors grpcDescriptors = createGrpcDescriptors();
        PipelineStepModel model = buildStepModel(
            Set.of(GenerationTarget.GRPC_SERVICE),
            DeploymentRole.PIPELINE_SERVER,
            false,
            false
        );

        PipelineCompilationContext ctx = newContext(tempDir, List.of(model), TransportMode.LOCAL);
        ctx.setRendererBindings(Map.of(
            model.serviceName() + "_grpc", new GrpcBinding(model, grpcDescriptors.service(), grpcDescriptors.method())
        ));

        phase.execute(ctx);

        assertFalse(Files.exists(tempDir.resolve(
            "pipeline-server/com/example/service/pipeline/TestServiceGrpcService.java")));
    }

    @Test
    void generatesClientStepInResolvedClientRoleDirectory(@TempDir Path tempDir) throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        GrpcDescriptors grpcDescriptors = createGrpcDescriptors();
        PipelineStepModel model = buildStepModel(
            Set.of(GenerationTarget.CLIENT_STEP),
            DeploymentRole.PIPELINE_SERVER,
            false,
            false
        );

        PipelineCompilationContext ctx = newContext(tempDir, List.of(model), TransportMode.GRPC);
        ctx.setRendererBindings(Map.of(
            model.serviceName() + "_grpc", new GrpcBinding(model, grpcDescriptors.service(), grpcDescriptors.method())
        ));

        phase.execute(ctx);

        assertTrue(Files.exists(tempDir.resolve(
            "orchestrator-client/com/example/service/pipeline/TestGrpcClientStep.java")));
    }

    @Test
    void generatesLocalClientStepWhenBindingPresent(@TempDir Path tempDir) throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        PipelineStepModel model = buildStepModel(
            Set.of(GenerationTarget.LOCAL_CLIENT_STEP),
            DeploymentRole.PIPELINE_SERVER,
            false,
            false
        );

        PipelineCompilationContext ctx = newContext(tempDir, List.of(model), TransportMode.LOCAL);
        ctx.setRendererBindings(Map.of(
            model.serviceName() + "_local", new LocalBinding(model)
        ));

        phase.execute(ctx);

        assertTrue(Files.exists(tempDir.resolve(
            "orchestrator-client/com/example/service/pipeline/TestLocalClientStep.java")));
    }

    @Test
    void warnsAndSkipsLocalClientStepWhenBindingMissing(@TempDir Path tempDir) throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        PipelineStepModel model = buildStepModel(
            Set.of(GenerationTarget.LOCAL_CLIENT_STEP),
            DeploymentRole.PIPELINE_SERVER,
            false,
            false
        );

        PipelineCompilationContext ctx = newContext(tempDir, List.of(model), TransportMode.LOCAL);
        ctx.setRendererBindings(Map.of());

        phase.execute(ctx);

        verify(messager).printMessage(
            eq(Diagnostic.Kind.WARNING),
            argThat(msg -> msg != null && msg.toString().contains("Skipping local client step generation"))
        );
        assertFalse(Files.exists(tempDir.resolve(
            "orchestrator-client/com/example/service/pipeline/TestLocalClientStep.java")));
    }

    @Test
    void generatesRestResourceAndRestClientArtifacts(@TempDir Path tempDir) throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        PipelineStepModel model = buildStepModel(
            Set.of(GenerationTarget.REST_RESOURCE, GenerationTarget.REST_CLIENT_STEP),
            DeploymentRole.PIPELINE_SERVER,
            false,
            true
        );

        PipelineCompilationContext ctx = newContext(tempDir, List.of(model), TransportMode.REST);
        ctx.setRendererBindings(Map.of(
            model.serviceName() + "_rest", new RestBinding(model, "/api/test")
        ));

        phase.execute(ctx);

        assertTrue(Files.exists(tempDir.resolve(
            "rest-server/com/example/service/pipeline/TestResource.java")));
        assertTrue(Files.exists(tempDir.resolve(
            "orchestrator-client/com/example/service/pipeline/TestRestClientStep.java")));
        assertTrue(Files.exists(tempDir.resolve(
            "orchestrator-client/com/example/service/pipeline/TestRestClient.java")));
    }

    @Test
    void suppressesClientGenerationForPluginServerInPluginHost(@TempDir Path tempDir) throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        GrpcDescriptors grpcDescriptors = createGrpcDescriptors();
        PipelineStepModel model = buildStepModel(
            Set.of(GenerationTarget.CLIENT_STEP),
            DeploymentRole.PLUGIN_SERVER,
            false,
            false
        );

        PipelineCompilationContext ctx = newContext(tempDir, List.of(model), TransportMode.GRPC);
        ctx.setPluginHost(true);
        ctx.setRendererBindings(Map.of(
            model.serviceName() + "_grpc", new GrpcBinding(model, grpcDescriptors.service(), grpcDescriptors.method())
        ));

        phase.execute(ctx);

        assertFalse(Files.exists(tempDir.resolve(
            "plugin-client/com/example/service/pipeline/TestGrpcClientStep.java")));
    }

    @Test
    void generatesProtobufParsersIncludingNestedMessages(@TempDir Path tempDir) throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        GrpcDescriptors grpcDescriptors = createGrpcDescriptors();
        PipelineStepModel model = buildStepModel(
            Set.of(GenerationTarget.CLIENT_STEP),
            DeploymentRole.PIPELINE_SERVER,
            false,
            false
        );

        PipelineCompilationContext ctx = newContext(tempDir, List.of(model), TransportMode.GRPC);
        ctx.setRendererBindings(Map.of(
            model.serviceName() + "_grpc", new GrpcBinding(model, grpcDescriptors.service(), grpcDescriptors.method())
        ));
        ctx.setDescriptorSet(grpcDescriptors.descriptorSet());

        phase.execute(ctx);

        assertTrue(Files.exists(tempDir.resolve(
            "pipeline-server/com/example/grpc/pipeline/ProtoInputMessageParser.java")));
        assertTrue(Files.exists(tempDir.resolve(
            "pipeline-server/com/example/grpc/pipeline/ProtoOuter_NestedParser.java")));
    }

    @Test
    void invalidDescriptorSetDoesNotFailGeneration(@TempDir Path tempDir) throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        PipelineStepModel model = buildStepModel(
            Set.of(GenerationTarget.LOCAL_CLIENT_STEP),
            DeploymentRole.PIPELINE_SERVER,
            false,
            false
        );

        PipelineCompilationContext ctx = newContext(tempDir, List.of(model), TransportMode.GRPC);
        ctx.setRendererBindings(Map.of(
            model.serviceName() + "_local", new LocalBinding(model)
        ));
        ctx.setDescriptorSet(createUnresolvableDescriptorSet());

        assertDoesNotThrow(() -> phase.execute(ctx));
        assertTrue(Files.exists(tempDir.resolve(
            "orchestrator-client/com/example/service/pipeline/TestLocalClientStep.java")));
    }

    @Test
    void orchestratorGenerationFlagWithMissingBindingDoesNotFail(@TempDir Path tempDir) {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        PipelineCompilationContext ctx = newContext(tempDir, List.of(), TransportMode.GRPC);
        ctx.setOrchestratorGenerated(true);
        ctx.setRendererBindings(Map.of());

        assertDoesNotThrow(() -> phase.execute(ctx));
    }

    @Test
    void warnsAndSkipsGrpcServiceWhenGrpcBindingMissing(@TempDir Path tempDir) throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        PipelineStepModel model = buildStepModel(
            Set.of(GenerationTarget.GRPC_SERVICE),
            DeploymentRole.PIPELINE_SERVER,
            false,
            false
        );
        PipelineCompilationContext ctx = newContext(tempDir, List.of(model), TransportMode.GRPC);
        ctx.setRendererBindings(Map.of());

        phase.execute(ctx);

        verify(messager).printMessage(
            eq(Diagnostic.Kind.WARNING),
            argThat(msg -> msg != null && msg.toString().contains("Skipping gRPC service generation"))
        );
        assertFalse(Files.exists(tempDir.resolve(
            "pipeline-server/com/example/service/pipeline/TestServiceGrpcService.java")));
    }

    @Test
    void warnsAndSkipsRestArtifactsWhenRestBindingMissing(@TempDir Path tempDir) throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        PipelineStepModel model = buildStepModel(
            Set.of(GenerationTarget.REST_RESOURCE, GenerationTarget.REST_CLIENT_STEP),
            DeploymentRole.PIPELINE_SERVER,
            false,
            true
        );
        PipelineCompilationContext ctx = newContext(tempDir, List.of(model), TransportMode.REST);
        ctx.setRendererBindings(Map.of());

        phase.execute(ctx);

        verify(messager).printMessage(
            eq(Diagnostic.Kind.WARNING),
            argThat(msg -> msg != null && msg.toString().contains("Skipping REST resource generation"))
        );
        verify(messager).printMessage(
            eq(Diagnostic.Kind.WARNING),
            argThat(msg -> msg != null && msg.toString().contains("Skipping REST client step generation"))
        );
        assertFalse(Files.exists(tempDir.resolve(
            "rest-server/com/example/service/pipeline/TestResource.java")));
        assertFalse(Files.exists(tempDir.resolve(
            "orchestrator-client/com/example/service/pipeline/TestRestClientStep.java")));
    }

    @Test
    void generatesPluginSideEffectBeanForLocalTransport(@TempDir Path tempDir) throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        PipelineStepModel model = buildStepModel(
            Set.of(GenerationTarget.GRPC_SERVICE),
            DeploymentRole.PLUGIN_SERVER,
            true,
            false
        );
        PipelineCompilationContext ctx = newContext(tempDir, List.of(model), TransportMode.LOCAL);
        ctx.setPluginHost(true);
        ctx.setRendererBindings(Map.of(
            model.serviceName() + "_grpc", new GrpcBinding(model, createGrpcDescriptors().service(), null)
        ));
        TypeElement typeElement = mock(TypeElement.class);
        ExecutableElement constructor = mock(ExecutableElement.class);
        when(constructor.getKind()).thenReturn(ElementKind.CONSTRUCTOR);
        when(constructor.getParameters()).thenReturn(List.of());
        doReturn(List.of(constructor)).when(typeElement).getEnclosedElements();
        when(elements.getTypeElement(model.serviceClassName().canonicalName())).thenReturn(typeElement);

        phase.execute(ctx);

        assertTrue(Files.exists(tempDir.resolve(
            "orchestrator-client/com/example/service/pipeline/TestService.java")));
    }

    @Test
    void executeCoversAspectNameMappingLambda(@TempDir Path tempDir) throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        GrpcDescriptors grpcDescriptors = createGrpcDescriptors();
        PipelineStepModel model = buildStepModel(
            Set.of(GenerationTarget.CLIENT_STEP),
            DeploymentRole.PIPELINE_SERVER,
            false,
            false
        );
        PipelineCompilationContext ctx = newContext(tempDir, List.of(model), TransportMode.GRPC);
        ctx.setAspectModels(List.of(
            new PipelineAspectModel("Retry", AspectScope.GLOBAL, AspectPosition.BEFORE_STEP, Map.of())
        ));
        ctx.setRendererBindings(Map.of(
            model.serviceName() + "_grpc", new GrpcBinding(model, grpcDescriptors.service(), grpcDescriptors.method())
        ));

        phase.execute(ctx);

        assertTrue(Files.exists(tempDir.resolve(
            "orchestrator-client/com/example/service/pipeline/TestGrpcClientStep.java")));
    }

    @Test
    void suppressesPluginServerGrpcAndRestWhenArtifactsNotAllowed(@TempDir Path tempDir) throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        GrpcDescriptors grpcDescriptors = createGrpcDescriptors();

        PipelineStepModel grpcModel = buildStepModel(
            Set.of(GenerationTarget.GRPC_SERVICE),
            DeploymentRole.PLUGIN_SERVER,
            false,
            false
        );
        PipelineStepModel restModel = buildStepModel(
            Set.of(GenerationTarget.REST_RESOURCE),
            DeploymentRole.PLUGIN_SERVER,
            false,
            true
        );

        PipelineCompilationContext ctx = newContext(tempDir, List.of(grpcModel, restModel), TransportMode.GRPC);
        ctx.setRendererBindings(Map.of(
            grpcModel.serviceName() + "_grpc", new GrpcBinding(grpcModel, grpcDescriptors.service(), grpcDescriptors.method()),
            restModel.serviceName() + "_rest", new RestBinding(restModel, "/api/test")
        ));

        phase.execute(ctx);

        assertFalse(Files.exists(tempDir.resolve(
            "plugin-server/com/example/service/pipeline/TestServiceGrpcService.java")));
        assertFalse(Files.exists(tempDir.resolve(
            "rest-server/com/example/service/pipeline/TestResource.java")));
    }

    @Test
    void allowsPluginServerArtifactsWhenRuntimeMappingAndModulePresent(@TempDir Path tempDir) throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        GrpcDescriptors grpcDescriptors = createGrpcDescriptors();
        PipelineStepModel model = buildStepModel(
            Set.of(GenerationTarget.GRPC_SERVICE),
            DeploymentRole.PLUGIN_SERVER,
            false,
            false
        );

        PipelineCompilationContext ctx = newContext(tempDir, List.of(model), TransportMode.GRPC);
        ctx.setModuleName("plugin-svc");
        ctx.setRuntimeMapping(new PipelineRuntimeMapping(
            null,
            null,
            null,
            Map.of("local", "local"),
            Map.of(),
            Map.of(),
            Map.of()));
        ctx.setRendererBindings(Map.of(
            model.serviceName() + "_grpc", new GrpcBinding(model, grpcDescriptors.service(), grpcDescriptors.method())
        ));

        phase.execute(ctx);

        assertTrue(Files.exists(tempDir.resolve(
            "plugin-server/com/example/service/pipeline/TestServiceGrpcService.java")));
    }

    @Test
    void generatesPluginClientStepWhenNotPluginHost(@TempDir Path tempDir) throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        GrpcDescriptors grpcDescriptors = createGrpcDescriptors();
        PipelineStepModel model = buildStepModel(
            Set.of(GenerationTarget.CLIENT_STEP),
            DeploymentRole.PLUGIN_SERVER,
            false,
            false
        );
        PipelineCompilationContext ctx = newContext(tempDir, List.of(model), TransportMode.GRPC);
        ctx.setRendererBindings(Map.of(
            model.serviceName() + "_grpc", new GrpcBinding(model, grpcDescriptors.service(), grpcDescriptors.method())
        ));

        phase.execute(ctx);

        assertTrue(Files.exists(tempDir.resolve(
            "plugin-client/com/example/service/pipeline/TestGrpcClientStep.java")));
    }

    @Test
    void suppressesPluginServerLocalClientWhenPluginHost(@TempDir Path tempDir) throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        PipelineStepModel model = buildStepModel(
            Set.of(GenerationTarget.LOCAL_CLIENT_STEP),
            DeploymentRole.PLUGIN_SERVER,
            false,
            false
        );
        PipelineCompilationContext ctx = newContext(tempDir, List.of(model), TransportMode.LOCAL);
        ctx.setPluginHost(true);
        ctx.setRendererBindings(Map.of(model.serviceName() + "_local", new LocalBinding(model)));

        phase.execute(ctx);

        assertFalse(Files.exists(tempDir.resolve(
            "plugin-client/com/example/service/pipeline/TestLocalClientStep.java")));
    }

    @Test
    void suppressesPluginServerRestClientWhenPluginHost(@TempDir Path tempDir) throws Exception {
        PipelineGenerationPhase phase = new PipelineGenerationPhase();
        PipelineStepModel model = buildStepModel(
            Set.of(GenerationTarget.REST_CLIENT_STEP),
            DeploymentRole.PLUGIN_SERVER,
            false,
            true
        );
        PipelineCompilationContext ctx = newContext(tempDir, List.of(model), TransportMode.REST);
        ctx.setPluginHost(true);
        ctx.setRendererBindings(Map.of(model.serviceName() + "_rest", new RestBinding(model, "/api/test")));

        phase.execute(ctx);

        assertFalse(Files.exists(tempDir.resolve(
            "plugin-client/com/example/service/pipeline/TestRestClientStep.java")));
    }

    private PipelineCompilationContext newContext(Path generatedSourcesRoot, List<PipelineStepModel> models,
                                                  TransportMode transportMode) {
        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        ctx.setGeneratedSourcesRoot(generatedSourcesRoot);
        ctx.setStepModels(models);
        ctx.setAspectModels(List.of());
        ctx.setTransportMode(transportMode);
        return ctx;
    }

    private PipelineStepModel buildStepModel(Set<GenerationTarget> targets, DeploymentRole role,
                                             boolean sideEffect, boolean includeRestMappers) {
        TypeMapping input = includeRestMappers
            ? new TypeMapping(
                ClassName.get("com.example.domain", "Input"),
                ClassName.get("com.example.mapper", "InputMapper"),
                true)
            : new TypeMapping(ClassName.get("com.example.domain", "Input"), null, false);
        TypeMapping output = includeRestMappers
            ? new TypeMapping(
                ClassName.get("com.example.domain", "Output"),
                ClassName.get("com.example.mapper", "OutputMapper"),
                true)
            : new TypeMapping(ClassName.get("com.example.domain", "Output"), null, false);
        return new PipelineStepModel(
            "TestService",
            "TestService",
            "com.example.service",
            ClassName.get("com.example.service", "TestService"),
            input,
            output,
            StreamingShape.UNARY_UNARY,
            targets,
            ExecutionMode.DEFAULT,
            role,
            sideEffect,
            null
        );
    }

    private GrpcDescriptors createGrpcDescriptors() throws Exception {
        DescriptorProtos.DescriptorProto inputMessage = DescriptorProtos.DescriptorProto.newBuilder()
            .setName("InputMessage")
            .build();
        DescriptorProtos.DescriptorProto outputMessage = DescriptorProtos.DescriptorProto.newBuilder()
            .setName("OutputMessage")
            .build();
        DescriptorProtos.DescriptorProto nestedMessage = DescriptorProtos.DescriptorProto.newBuilder()
            .setName("Nested")
            .build();
        DescriptorProtos.DescriptorProto outerMessage = DescriptorProtos.DescriptorProto.newBuilder()
            .setName("Outer")
            .addNestedType(nestedMessage)
            .build();

        DescriptorProtos.MethodDescriptorProto remoteProcess = DescriptorProtos.MethodDescriptorProto.newBuilder()
            .setName("remoteProcess")
            .setInputType(".example.InputMessage")
            .setOutputType(".example.OutputMessage")
            .build();

        DescriptorProtos.ServiceDescriptorProto service = DescriptorProtos.ServiceDescriptorProto.newBuilder()
            .setName("TestService")
            .addMethod(remoteProcess)
            .build();

        DescriptorProtos.FileOptions options = DescriptorProtos.FileOptions.newBuilder()
            .setJavaPackage("com.example.grpc")
            .setJavaMultipleFiles(true)
            .build();

        DescriptorProtos.FileDescriptorProto fileProto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("proto/test.proto")
            .setPackage("example")
            .setOptions(options)
            .addMessageType(inputMessage)
            .addMessageType(outputMessage)
            .addMessageType(outerMessage)
            .addService(service)
            .build();

        Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(
            fileProto,
            new Descriptors.FileDescriptor[0]
        );
        Descriptors.ServiceDescriptor serviceDescriptor = fileDescriptor.findServiceByName("TestService");
        Descriptors.MethodDescriptor methodDescriptor = serviceDescriptor.findMethodByName("remoteProcess");

        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(fileProto)
            .build();

        return new GrpcDescriptors(descriptorSet, serviceDescriptor, methodDescriptor);
    }

    private DescriptorProtos.FileDescriptorSet createUnresolvableDescriptorSet() {
        DescriptorProtos.FileDescriptorProto unresolved = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("proto/unresolved.proto")
            .addDependency("proto/missing.proto")
            .build();
        return DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(unresolved)
            .build();
    }

    private record GrpcDescriptors(
        DescriptorProtos.FileDescriptorSet descriptorSet,
        Descriptors.ServiceDescriptor service,
        Descriptors.MethodDescriptor method
    ) {}
}
