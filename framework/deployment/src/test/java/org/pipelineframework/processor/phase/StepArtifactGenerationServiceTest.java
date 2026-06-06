package org.pipelineframework.processor.phase;

import java.nio.file.Path;
import java.nio.file.Files;
import java.util.Set;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.util.Elements;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
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
import org.pipelineframework.processor.renderer.ClientStepRenderer;
import org.pipelineframework.processor.renderer.BlockingReactiveBridgeRenderer;
import org.pipelineframework.processor.renderer.GenerationContext;
import org.pipelineframework.processor.renderer.GrpcServiceAdapterRenderer;
import org.pipelineframework.processor.renderer.LocalClientStepRenderer;
import org.pipelineframework.processor.renderer.RemoteOperatorAdapterRenderer;
import org.pipelineframework.processor.renderer.RestClientStepRenderer;
import org.pipelineframework.processor.renderer.RestFunctionHandlerRenderer;
import org.pipelineframework.processor.renderer.RestResourceRenderer;
import org.pipelineframework.processor.util.RoleMetadataGenerator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

    @Mock
    private Elements elements;

    @TempDir
    Path tempDir;

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

        PipelineStepModel model = buildChargeCardModel().toBuilder()
            .remoteExecution(new PipelineTemplateStepExecution(
                "REMOTE",
                "charge-card",
                "PROTOBUF_HTTP_V1",
                3000,
                new PipelineTemplateRemoteTarget("https://operator.example/process", null)))
            .build();

        RemoteOperatorAdapterRenderer renderer = mock(RemoteOperatorAdapterRenderer.class);

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
            mock(GrpcServiceAdapterRenderer.class),
            mock(ClientStepRenderer.class),
            mock(LocalClientStepRenderer.class),
            mock(RestClientStepRenderer.class),
            mock(RestResourceRenderer.class),
            mock(RestFunctionHandlerRenderer.class),
            mock(BlockingReactiveBridgeRenderer.class),
            renderer
        );

        ArgumentCaptor<GrpcBinding> bindingCaptor = ArgumentCaptor.forClass(GrpcBinding.class);
        ArgumentCaptor<GenerationContext> contextCaptor = ArgumentCaptor.forClass(GenerationContext.class);
        verify(renderer).render(bindingCaptor.capture(), contextCaptor.capture());
        assertEquals(
            "ChargeCard",
            assertInstanceOf(Descriptors.ServiceDescriptor.class, bindingCaptor.getValue().serviceDescriptor()).getName());
        assertEquals(DeploymentRole.PIPELINE_SERVER, contextCaptor.getValue().role());
    }

    @Test
    void localSideEffectOnlyTargetGeneratesObservedBean() throws Exception {
        when(processingEnv.getElementUtils()).thenReturn(elements);

        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, null);
        ctx.setGeneratedSourcesRoot(tempDir.resolve("generated-sources"));

        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ObservePersistenceInvoiceApprovalSideEffectService")
            .generatedName("PersistenceInvoiceApprovalSideEffect")
            .servicePackage("com.example.invoice")
            .serviceClassName(ClassName.get("org.pipelineframework.plugin.persistence", "PersistenceService"))
            .inputMapping(new TypeMapping(ClassName.get("com.example.invoice.domain", "InvoiceApproval"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.invoice.domain", "InvoiceApproval"), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(Set.of(GenerationTarget.GRPC_SERVICE_SIDE_EFFECT_ONLY))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .sideEffect(true)
            .build();

        invokeGenerateArtifacts(ctx, model);

        Path generatedBean = tempDir.resolve(
            "generated-sources/orchestrator-client/com/example/invoice/pipeline/ObservePersistenceInvoiceApprovalSideEffectService.java");
        assertTrue(Files.exists(generatedBean));
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
            mock(GrpcServiceAdapterRenderer.class),
            mock(ClientStepRenderer.class),
            mock(LocalClientStepRenderer.class),
            mock(RestClientStepRenderer.class),
            mock(RestResourceRenderer.class),
            mock(RestFunctionHandlerRenderer.class),
            mock(BlockingReactiveBridgeRenderer.class),
            mock(RemoteOperatorAdapterRenderer.class)
        );
    }

    private GrpcBinding grpcBinding() {
        Descriptors.FileDescriptor fileDescriptor = buildFileDescriptor();
        Descriptors.ServiceDescriptor serviceDescriptor = fileDescriptor.findServiceByName("ChargeCard");
        Descriptors.MethodDescriptor methodDescriptor = serviceDescriptor.findMethodByName("remoteProcess");
        PipelineStepModel model = buildChargeCardModel();
        return new GrpcBinding(model, serviceDescriptor, methodDescriptor);
    }

    private PipelineStepModel buildChargeCardModel() {
        return new PipelineStepModel.Builder()
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
