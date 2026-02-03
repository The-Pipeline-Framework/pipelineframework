package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.processing.ProcessingEnvironment;

import com.google.protobuf.DescriptorProtos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.*;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrchestratorIngestClientRendererTest {

    @TempDir
    Path tempDir;

    @Test
    void rendersIngestClient() throws IOException {
        OrchestratorBinding binding = buildBinding();
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet();
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));
        when(processingEnv.getMessager()).thenReturn(mock(javax.annotation.processing.Messager.class));

        OrchestratorIngestClientRenderer renderer = new OrchestratorIngestClientRenderer();
        renderer.render(binding, new GenerationContext(processingEnv, tempDir, DeploymentRole.ORCHESTRATOR_CLIENT,
            java.util.Set.of(), null, descriptorSet));

        Path generatedSource = tempDir.resolve("com/example/orchestrator/client/OrchestratorIngestClient.java");
        String source = Files.readString(generatedSource);

        assertTrue(source.contains("class OrchestratorIngestClient"));
        assertTrue(source.contains("@GrpcClient(\"orchestrator\")"));
        assertTrue(source.contains("public Multi<OutputType> ingest(Multi<InputType> input)"));
        assertTrue(source.contains("return grpcClient.ingest(input);"));
        assertTrue(source.contains("public Multi<OutputType> subscribe("));
        assertTrue(source.contains("return grpcClient.subscribe(SubscribeInputType.getDefaultInstance())"));
    }

    private OrchestratorBinding buildBinding() {
        PipelineStepModel model = new PipelineStepModel(
            "OrchestratorService",
            "OrchestratorService",
            "com.example.orchestrator.service",
            com.squareup.javapoet.ClassName.get("com.example.orchestrator.service", "OrchestratorService"),
            null,
            null,
            StreamingShape.UNARY_UNARY,
            java.util.Set.of(GenerationTarget.GRPC_SERVICE),
            ExecutionMode.DEFAULT,
            DeploymentRole.ORCHESTRATOR_CLIENT,
            false,
            null
        );

        return new OrchestratorBinding(
            model,
            "com.example",
            "GRPC",
            "InputType",
            "OutputType",
            false,
            false,
            "ProcessAlphaService",
            StreamingShape.UNARY_UNARY,
            null,
            null,
            null
        );
    }

    private DescriptorProtos.FileDescriptorSet buildDescriptorSet() {
        DescriptorProtos.FileDescriptorProto proto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("orchestrator.proto")
            .setPackage("com.example.grpc")
            .setOptions(DescriptorProtos.FileOptions.newBuilder()
                .setJavaPackage("com.example.grpc")
                .setJavaOuterClassname("MutinyOrchestratorServiceGrpc")
                .setJavaMultipleFiles(true)
                .build())
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                .setName("InputType"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                .setName("SubscribeInputType"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                .setName("OutputType"))
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("OrchestratorService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Run")
                    .setInputType(".com.example.grpc.InputType")
                    .setOutputType(".com.example.grpc.OutputType"))
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Ingest")
                    .setInputType(".com.example.grpc.InputType")
                    .setOutputType(".com.example.grpc.OutputType")
                    .setClientStreaming(true)
                    .setServerStreaming(true))
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Subscribe")
                    .setInputType(".com.example.grpc.SubscribeInputType")
                    .setOutputType(".com.example.grpc.OutputType")
                    .setClientStreaming(false)
                    .setServerStreaming(true)))
            .build();

        return DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(proto)
            .build();
    }
}
