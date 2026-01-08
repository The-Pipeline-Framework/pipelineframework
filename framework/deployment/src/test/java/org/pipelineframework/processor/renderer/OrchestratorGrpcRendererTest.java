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

class OrchestratorGrpcRendererTest {

    @TempDir
    Path tempDir;

    @Test
    void rendersUnaryGrpcService() throws IOException {
        OrchestratorBinding binding = buildBinding(false, false);
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(false, false);
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));
        when(processingEnv.getMessager()).thenReturn(null);

        OrchestratorGrpcRenderer renderer = new OrchestratorGrpcRenderer();
        renderer.render(binding, new GenerationContext(processingEnv, tempDir, DeploymentRole.PIPELINE_SERVER,
            java.util.Set.of(), null, descriptorSet));

        Path generatedSource = tempDir.resolve("com/example/orchestrator/service/OrchestratorGrpcService.java");
        String source = Files.readString(generatedSource);

        assertTrue(source.contains("package com.example.orchestrator.service;"));
        assertTrue(source.contains("@GrpcService"));
        assertTrue(source.contains("extends MutinyOrchestratorServiceGrpc.OrchestratorServiceImplBase"));
        assertTrue(source.contains("public Uni<OutputType> run(InputType input)"));
        assertTrue(source.contains("return pipelineExecutionService.executePipelineUnary("));
    }

    @Test
    void rendersStreamingGrpcService() throws IOException {
        OrchestratorBinding binding = buildBinding(true, true);
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(true, true);
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));
        when(processingEnv.getMessager()).thenReturn(null);

        OrchestratorGrpcRenderer renderer = new OrchestratorGrpcRenderer();
        renderer.render(binding, new GenerationContext(processingEnv, tempDir, DeploymentRole.PIPELINE_SERVER,
            java.util.Set.of(), null, descriptorSet));

        Path generatedSource = tempDir.resolve("com/example/orchestrator/service/OrchestratorGrpcService.java");
        String source = Files.readString(generatedSource);

        assertTrue(source.contains("public Multi<OutputType> run(Multi<InputType> input)"));
        assertTrue(source.contains("return pipelineExecutionService.executePipelineStreaming(input);"));
    }

    private OrchestratorBinding buildBinding(boolean inputStreaming, boolean outputStreaming) {
        PipelineStepModel model = new PipelineStepModel(
            "OrchestratorService",
            "OrchestratorService",
            "com.example.orchestrator.service",
            com.squareup.javapoet.ClassName.get("com.example.orchestrator.service", "OrchestratorService"),
            null,
            null,
            streamingShape(inputStreaming, outputStreaming),
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
            inputStreaming,
            outputStreaming,
            "ProcessAlphaService",
            StreamingShape.UNARY_UNARY,
            null,
            null,
            null
        );
    }

    private DescriptorProtos.FileDescriptorSet buildDescriptorSet(boolean inputStreaming, boolean outputStreaming) {
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
                .setName("OrchestratorService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Run")
                    .setInputType(".com.example.grpc.InputType")
                    .setOutputType(".com.example.grpc.OutputType")
                    .setClientStreaming(inputStreaming)
                    .setServerStreaming(outputStreaming)))
            .build();

        return DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(proto)
            .build();
    }

    private StreamingShape streamingShape(boolean inputStreaming, boolean outputStreaming) {
        if (inputStreaming && outputStreaming) {
            return StreamingShape.STREAMING_STREAMING;
        }
        if (inputStreaming) {
            return StreamingShape.STREAMING_UNARY;
        }
        if (outputStreaming) {
            return StreamingShape.UNARY_STREAMING;
        }
        return StreamingShape.UNARY_UNARY;
    }
}
