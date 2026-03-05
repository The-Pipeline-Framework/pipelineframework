package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.processing.ProcessingEnvironment;

import com.google.protobuf.DescriptorProtos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        assertTrue(source.contains("executePipelineUnary"));
        assertTrue(source.contains("public Uni<RunAsyncResponse> runAsync(RunAsyncRequest request)"));
        assertTrue(source.contains("executePipelineAsync"));
        assertTrue(source.contains("RunAsync unary pipelines accept at most one item in input_batch."));
        assertTrue(source.contains("public Uni<GetExecutionStatusResponse> getExecutionStatus(GetExecutionStatusRequest request)"));
        assertTrue(source.contains("public Uni<GetExecutionResultResponse> getExecutionResult(GetExecutionResultRequest request)"));
        assertTrue(source.contains("executionId is required"));
        assertTrue(source.contains("public Multi<OutputType> ingest(Multi<InputType> input)"));
        assertTrue(source.contains("public Multi<OutputType> subscribe("));
        assertTrue(source.contains("pipelineOutputBus"));
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
        assertTrue(source.contains("executePipelineStreaming"));
        assertTrue(source.contains("public Uni<RunAsyncResponse> runAsync(RunAsyncRequest request)"));
        assertTrue(source.contains("executePipelineAsync"));
        assertTrue(source.contains("public Multi<OutputType> ingest(Multi<InputType> input)"));
        assertTrue(source.contains("public Multi<OutputType> subscribe("));
        assertTrue(source.contains("pipelineOutputBus"));
    }

    @Test
    void rendersRpcMetricsTelemetry() throws IOException {
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

        assertTrue(source.contains("RpcMetrics.recordGrpcServer"));
        assertTrue(source.contains("System.nanoTime() - startTime"));
        assertTrue(source.contains("Status.OK"));
        assertTrue(source.contains("Status.fromThrowable"));
    }

    @Test
    void rendersSubscribeWithPipelineContextCheck() throws IOException {
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

        assertTrue(source.contains("if (PipelineContextHolder.get() == null)"));
        assertTrue(source.contains("Missing pipeline context for subscribe request"));
    }

    @Test
    void rendersRunAsyncWithTenantAndIdempotency() throws IOException {
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

        assertTrue(source.contains("request.getTenantId()"));
        assertTrue(source.contains("request.getIdempotencyKey()"));
        assertTrue(source.contains("RunAsyncResponse.newBuilder()"));
        assertTrue(source.contains(".setExecutionId(accepted.executionId())"));
        assertTrue(source.contains(".setDuplicate(accepted.duplicate())"));
    }

    @Test
    void rendersGetExecutionStatusWithProperMapping() throws IOException {
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

        assertTrue(source.contains("pipelineExecutionService.getExecutionStatus"));
        assertTrue(source.contains(".setExecutionId(status.executionId())"));
        assertTrue(source.contains(".setStatus(status.status().name())"));
        assertTrue(source.contains(".setCurrentStepIndex(status.stepIndex())"));
        assertTrue(source.contains(".setAttempt(status.attempt())"));
    }

    @Test
    void rendersGetExecutionResultWithStreamingOutput() throws IOException {
        OrchestratorBinding binding = buildBinding(false, true);
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(false, true);
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));
        when(processingEnv.getMessager()).thenReturn(null);

        OrchestratorGrpcRenderer renderer = new OrchestratorGrpcRenderer();
        renderer.render(binding, new GenerationContext(processingEnv, tempDir, DeploymentRole.PIPELINE_SERVER,
            java.util.Set.of(), null, descriptorSet));

        Path generatedSource = tempDir.resolve("com/example/orchestrator/service/OrchestratorGrpcService.java");
        String source = Files.readString(generatedSource);

        assertTrue(source.contains("pipelineExecutionService.<List<OutputType>>getExecutionResult"));
        assertTrue(source.contains(".addAllItems(items)"));
    }

    @Test
    void rendersGetExecutionResultWithUnaryOutput() throws IOException {
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

        assertTrue(source.contains("if (result != null)"));
        assertTrue(source.contains("builder.addItems(result)"));
    }

    @Test
    void throwsExceptionWhenDescriptorSetIsNull() {
        OrchestratorBinding binding = buildBinding(false, false);
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));
        when(processingEnv.getMessager()).thenReturn(null);

        OrchestratorGrpcRenderer renderer = new OrchestratorGrpcRenderer();

        assertThrows(IllegalStateException.class, () ->
            renderer.render(binding, new GenerationContext(processingEnv, tempDir, DeploymentRole.PIPELINE_SERVER,
                java.util.Set.of(), null, null)));
    }

    @Test
    void rendererReturnsGrpcServiceGenerationTarget() {
        OrchestratorGrpcRenderer renderer = new OrchestratorGrpcRenderer();
        assertEquals(GenerationTarget.GRPC_SERVICE, renderer.target());
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
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                .setName("RunAsyncRequest")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("input")
                    .setNumber(1)
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName(".com.example.grpc.InputType"))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("input_batch")
                    .setNumber(2)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName(".com.example.grpc.InputType"))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("tenant_id")
                    .setNumber(3)
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("idempotency_key")
                    .setNumber(4)
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                .setName("RunAsyncResponse"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                .setName("GetExecutionStatusRequest"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                .setName("GetExecutionStatusResponse"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                .setName("GetExecutionResultRequest"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                .setName("GetExecutionResultResponse")
                .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("items")
                    .setNumber(1)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName(".com.example.grpc.OutputType")))
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("OrchestratorService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Run")
                    .setInputType(".com.example.grpc.InputType")
                    .setOutputType(".com.example.grpc.OutputType")
                    .setClientStreaming(inputStreaming)
                    .setServerStreaming(outputStreaming))
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Ingest")
                    .setInputType(".com.example.grpc.InputType")
                    .setOutputType(".com.example.grpc.OutputType")
                    .setClientStreaming(true)
                    .setServerStreaming(true))
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("RunAsync")
                    .setInputType(".com.example.grpc.RunAsyncRequest")
                    .setOutputType(".com.example.grpc.RunAsyncResponse"))
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("GetExecutionStatus")
                    .setInputType(".com.example.grpc.GetExecutionStatusRequest")
                    .setOutputType(".com.example.grpc.GetExecutionStatusResponse"))
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("GetExecutionResult")
                    .setInputType(".com.example.grpc.GetExecutionResultRequest")
                    .setOutputType(".com.example.grpc.GetExecutionResultResponse"))
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Subscribe")
                    .setInputType(".com.example.grpc.InputType")
                    .setOutputType(".com.example.grpc.OutputType")
                    .setClientStreaming(false)
                    .setServerStreaming(true)))
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
