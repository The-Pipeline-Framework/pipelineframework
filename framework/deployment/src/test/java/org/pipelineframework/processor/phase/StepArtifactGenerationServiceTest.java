package org.pipelineframework.processor.phase;

import java.nio.file.Path;
import java.util.Set;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.config.template.PipelineTemplateRemoteTarget;
import org.pipelineframework.config.template.PipelineTemplateStepExecution;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.processor.util.RoleMetadataGenerator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StepArtifactGenerationServiceTest {

    @Mock
    private ProcessingEnvironment processingEnv;

    @Mock
    private Messager messager;

    private StepArtifactGenerationService service;

    @BeforeEach
    void setUp() {
        service = new StepArtifactGenerationService(
            new GenerationPathResolver(),
            new GenerationPolicy(),
            new SideEffectBeanService(new GenerationPathResolver()));
    }

    @Test
    void constructorRejectsNullDependencies() {
        assertThrows(NullPointerException.class,
            () -> new StepArtifactGenerationService(null, new GenerationPolicy(), new SideEffectBeanService(new GenerationPathResolver())));
        assertThrows(NullPointerException.class,
            () -> new StepArtifactGenerationService(new GenerationPathResolver(), null, new SideEffectBeanService(new GenerationPathResolver())));
        assertThrows(NullPointerException.class,
            () -> new StepArtifactGenerationService(new GenerationPathResolver(), new GenerationPolicy(), null));
    }

    @Test
    void clientStepWithoutGrpcBindingIsSkipped() {
        when(processingEnv.getMessager()).thenReturn(messager);
        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, null);
        ctx.setGeneratedSourcesRoot(Path.of("target/generated-sources-test"));

        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ProcessMissingBindingService")
            .generatedName("ProcessMissingBindingService")
            .servicePackage("com.example")
            .serviceClassName(ClassName.get("com.example", "MissingBindingService"))
            .inputMapping(new TypeMapping(ClassName.get("com.example", "In"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example", "Out"), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of(GenerationTarget.CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .sideEffect(false)
            .build();

        assertDoesNotThrow(() -> invokeGenerateArtifacts(ctx, model));

        verify(messager).printMessage(
            eq(javax.tools.Diagnostic.Kind.WARNING),
            contains("Skipping gRPC client step generation"));
    }

    @Test
    void remoteOperatorTargetInvokesRemoteAdapterRenderer() throws Exception {
        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, null);
        ctx.setGeneratedSourcesRoot(Path.of("target/generated-sources-test"));

        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ChargeCard")
            .generatedName("ChargeCard")
            .servicePackage("com.example.checkout")
            .serviceClassName(ClassName.get("com.example.checkout.pipeline", "ChargeCardRemoteOperatorAdapter"))
            .inputMapping(new TypeMapping(ClassName.get("com.example.checkout.domain", "ChargeRequest"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.checkout.domain", "ChargeResult"), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of(GenerationTarget.REMOTE_OPERATOR_ADAPTER))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .sideEffect(false)
            .remoteExecution(new PipelineTemplateStepExecution(
                "REMOTE",
                "charge-card",
                "PROTOBUF_HTTP_V1",
                3000,
                new PipelineTemplateRemoteTarget("https://operator.example/process", null)))
            .build();

        org.pipelineframework.processor.renderer.RemoteOperatorAdapterRenderer renderer =
            mock(org.pipelineframework.processor.renderer.RemoteOperatorAdapterRenderer.class);

        service.generateArtifactsForModel(
            ctx,
            model,
            grpcBinding(),
            null,
            null,
            new java.util.HashSet<>(),
            Set.of(),
            null,
            null,
            new RoleMetadataGenerator(processingEnv),
            mock(org.pipelineframework.processor.renderer.GrpcServiceAdapterRenderer.class),
            mock(org.pipelineframework.processor.renderer.ClientStepRenderer.class),
            mock(org.pipelineframework.processor.renderer.LocalClientStepRenderer.class),
            mock(org.pipelineframework.processor.renderer.RestClientStepRenderer.class),
            mock(org.pipelineframework.processor.renderer.RestResourceRenderer.class),
            mock(org.pipelineframework.processor.renderer.RestFunctionHandlerRenderer.class),
            renderer
        );

        ArgumentCaptor<GrpcBinding> bindingCaptor = ArgumentCaptor.forClass(GrpcBinding.class);
        ArgumentCaptor<org.pipelineframework.processor.renderer.GenerationContext> contextCaptor =
            ArgumentCaptor.forClass(org.pipelineframework.processor.renderer.GenerationContext.class);
        verify(renderer).render(bindingCaptor.capture(), contextCaptor.capture());
        assertEquals("ChargeCard", ((Descriptors.ServiceDescriptor) bindingCaptor.getValue().serviceDescriptor()).getName());
        assertEquals(DeploymentRole.PIPELINE_SERVER, contextCaptor.getValue().role());
    }

    private void invokeGenerateArtifacts(PipelineCompilationContext ctx, PipelineStepModel model) throws Exception {
        service.generateArtifactsForModel(
            ctx,
            model,
            null,
            null,
            null,
            new java.util.HashSet<>(),
            Set.of(),
            null,
            null,
            new RoleMetadataGenerator(processingEnv),
            mock(org.pipelineframework.processor.renderer.GrpcServiceAdapterRenderer.class),
            mock(org.pipelineframework.processor.renderer.ClientStepRenderer.class),
            mock(org.pipelineframework.processor.renderer.LocalClientStepRenderer.class),
            mock(org.pipelineframework.processor.renderer.RestClientStepRenderer.class),
            mock(org.pipelineframework.processor.renderer.RestResourceRenderer.class),
            mock(org.pipelineframework.processor.renderer.RestFunctionHandlerRenderer.class),
            mock(org.pipelineframework.processor.renderer.RemoteOperatorAdapterRenderer.class)
        );
    }

    private GrpcBinding grpcBinding() {
        Descriptors.FileDescriptor fileDescriptor = buildFileDescriptor();
        Descriptors.ServiceDescriptor serviceDescriptor = fileDescriptor.findServiceByName("ChargeCard");
        Descriptors.MethodDescriptor methodDescriptor = serviceDescriptor.findMethodByName("remoteProcess");
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ChargeCard")
            .generatedName("ChargeCard")
            .servicePackage("com.example.checkout")
            .serviceClassName(ClassName.get("com.example.checkout.pipeline", "ChargeCardRemoteOperatorAdapter"))
            .inputMapping(new TypeMapping(ClassName.get("com.example.checkout.domain", "ChargeRequest"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.checkout.domain", "ChargeResult"), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of(GenerationTarget.REMOTE_OPERATOR_ADAPTER))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .sideEffect(false)
            .build();
        return new GrpcBinding(model, serviceDescriptor, methodDescriptor);
    }

    private Descriptors.FileDescriptor buildFileDescriptor() {
        DescriptorProtos.FileDescriptorProto proto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("charge_card.proto")
            .setPackage("com.example.checkout.proto")
            .setOptions(DescriptorProtos.FileOptions.newBuilder()
                .setJavaPackage("com.example.checkout.proto")
                .setJavaOuterClassname("ChargeCardProto")
                .build())
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("ChargeRequest"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("ChargeResult"))
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("ChargeCard")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("remoteProcess")
                    .setInputType(".com.example.checkout.proto.ChargeRequest")
                    .setOutputType(".com.example.checkout.proto.ChargeResult")))
            .build();

        try {
            return Descriptors.FileDescriptor.buildFrom(proto, new Descriptors.FileDescriptor[] {});
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException("Failed to build remote operator test descriptor", e);
        }
    }
}
