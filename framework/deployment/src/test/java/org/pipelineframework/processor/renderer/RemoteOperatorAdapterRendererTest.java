package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.processing.ProcessingEnvironment;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.template.PipelineTemplateRemoteTarget;
import org.pipelineframework.config.template.PipelineTemplateStepExecution;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RemoteOperatorAdapterRendererTest {

    @TempDir
    Path tempDir;

    @Test
    void rendersRemoteAdapterUsingRuntimeConfigKey() throws IOException {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));

        RemoteOperatorAdapterRenderer renderer = new RemoteOperatorAdapterRenderer();
        renderer.render(
            new GrpcBinding(remoteModel(new PipelineTemplateRemoteTarget(null, "tpf.remote-operators.charge-card.url")),
                serviceDescriptor(),
                methodDescriptor()),
            new GenerationContext(processingEnv, tempDir, DeploymentRole.PIPELINE_SERVER,
                java.util.Set.of(), null, null));

        Path generatedSource =
            tempDir.resolve("org/example/checkout/service/pipeline/ChargeCardRemoteOperatorAdapter.java");
        String source = Files.readString(generatedSource);

        assertTrue(source.contains("class ChargeCardRemoteOperatorAdapter"));
        assertTrue(source.contains("ProtobufHttpRemoteOperatorClient remoteOperatorClient"));
        assertTrue(source.contains("@ConfigProperty("));
        assertTrue(source.contains("name = \"tpf.remote-operators.charge-card.url\""));
        assertTrue(source.contains("@PostConstruct"));
        assertTrue(source.contains("void validateRemoteTarget()"));
        assertTrue(source.contains("configuredTargetUrl.orElse(null)"));
        assertTrue(source.contains("requestMapper.toExternal(input)"));
        assertTrue(source.contains("remoteOperatorClient.invoke(resolveTargetUrl(), \"charge-card\", request.toByteArray(), 3000)"));
        assertTrue(source.contains("responseMapper.fromExternal(decodeResponse(bytes))"));
    }

    @Test
    void rendersRemoteAdapterUsingLiteralUrl() throws IOException {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));

        RemoteOperatorAdapterRenderer renderer = new RemoteOperatorAdapterRenderer();
        renderer.render(
            new GrpcBinding(remoteModel(new PipelineTemplateRemoteTarget("https://operator.example/process", null)),
                serviceDescriptor(),
                methodDescriptor()),
            new GenerationContext(processingEnv, tempDir, DeploymentRole.PIPELINE_SERVER,
                java.util.Set.of(), null, null));

        Path generatedSource =
            tempDir.resolve("org/example/checkout/service/pipeline/ChargeCardRemoteOperatorAdapter.java");
        String source = Files.readString(generatedSource);

        assertTrue(source.contains("return \"https://operator.example/process\""));
        assertFalse(source.contains("@ConfigProperty("));
        assertFalse(source.contains("configuredTargetUrl"));
    }

    private PipelineStepModel remoteModel(PipelineTemplateRemoteTarget target) {
        return new PipelineStepModel.Builder()
            .serviceName("ChargeCard")
            .generatedName("ChargeCard")
            .servicePackage("org.example.checkout.service")
            .serviceClassName(ClassName.get("org.example.checkout.service.pipeline", "ChargeCardRemoteOperatorAdapter"))
            .inputMapping(new TypeMapping(
                ClassName.get("org.example.checkout.domain", "ChargeRequest"),
                ClassName.get("org.example.checkout.mapper", "ChargeRequestMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.example.checkout.domain", "ChargeResult"),
                ClassName.get("org.example.checkout.mapper", "ChargeResultMapper"),
                true))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .enabledTargets(java.util.Set.of(GenerationTarget.REMOTE_OPERATOR_ADAPTER))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .remoteExecution(new PipelineTemplateStepExecution(
                "REMOTE",
                "charge-card",
                "PROTOBUF_HTTP_V1",
                3000,
                target))
            .build();
    }

    private Descriptors.ServiceDescriptor serviceDescriptor() {
        return fileDescriptor().findServiceByName("ChargeCard");
    }

    private Descriptors.MethodDescriptor methodDescriptor() {
        return serviceDescriptor().findMethodByName("remoteProcess");
    }

    private Descriptors.FileDescriptor fileDescriptor() {
        DescriptorProtos.FileDescriptorProto proto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("charge_card.proto")
            .setPackage("org.example.checkout.proto")
            .setOptions(DescriptorProtos.FileOptions.newBuilder()
                .setJavaPackage("org.example.checkout.proto")
                .setJavaOuterClassname("ChargeCardProto")
                .build())
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("ChargeRequest"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("ChargeResult"))
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("ChargeCard")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("remoteProcess")
                    .setInputType(".org.example.checkout.proto.ChargeRequest")
                    .setOutputType(".org.example.checkout.proto.ChargeResult")))
            .build();
        try {
            return Descriptors.FileDescriptor.buildFrom(proto, new Descriptors.FileDescriptor[] {});
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException("Failed to build remote operator test descriptor", e);
        }
    }
}
