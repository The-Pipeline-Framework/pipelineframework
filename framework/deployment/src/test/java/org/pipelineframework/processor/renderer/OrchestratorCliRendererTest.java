package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.processing.ProcessingEnvironment;

import com.google.protobuf.DescriptorProtos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.*;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrchestratorCliRendererTest {

    @TempDir
    Path tempDir;

    @Test
    void rendersGrpcCliWithMapper() throws IOException {
        OrchestratorBinding binding = buildBinding("GRPC");
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet();
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));
        when(processingEnv.getMessager()).thenReturn(null);

        OrchestratorCliRenderer renderer = new OrchestratorCliRenderer();
        renderer.render(binding, new GenerationContext(processingEnv, tempDir, DeploymentRole.ORCHESTRATOR_CLIENT,
            java.util.Set.of(), null, descriptorSet));

        Path generatedSource = tempDir.resolve("com/example/orchestrator/OrchestratorApplication.java");
        String source = Files.readString(generatedSource);

        assertTrue(source.contains("Multi<InputType> inputMulti"));
        assertTrue(source.contains("InputTypeMapper inputTypeMapper"));
        assertTrue(source.contains("InputTypeDto.class"));
        assertTrue(source.contains(".map(inputTypeMapper::toGrpc)"));
    }

    @Test
    void rendersRestCliWithoutMapper() throws IOException {
        OrchestratorBinding binding = buildBinding("REST");
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));

        OrchestratorCliRenderer renderer = new OrchestratorCliRenderer();
        renderer.render(binding, new GenerationContext(processingEnv, tempDir, DeploymentRole.ORCHESTRATOR_CLIENT,
            java.util.Set.of(), null, null));

        Path generatedSource = tempDir.resolve("com/example/orchestrator/OrchestratorApplication.java");
        String source = Files.readString(generatedSource);

        assertTrue(source.contains("Multi<InputTypeDto> inputMulti"));
        assertTrue(source.contains("InputTypeDto.class"));
        assertFalse(source.contains("Mapper"));
    }

    private OrchestratorBinding buildBinding(String transport) {
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
            transport,
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
                .setName("OutputType"))
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("ProcessAlphaService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("remoteProcess")
                    .setInputType(".com.example.grpc.InputType")
                    .setOutputType(".com.example.grpc.OutputType")))
            .build();

        return DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(proto)
            .build();
    }
}
