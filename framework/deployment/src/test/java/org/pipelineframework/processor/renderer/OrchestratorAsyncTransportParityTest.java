package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;

import com.google.protobuf.DescriptorProtos;
import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.OrchestratorBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrchestratorAsyncTransportParityTest {

    @TempDir
    Path tempDir;

    @Test
    void rendersNativeAsyncSurfaceAcrossRestGrpcAndFunction() throws IOException {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        Messager messager = mock(Messager.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));
        when(processingEnv.getMessager()).thenReturn(messager);

        OrchestratorBinding restBinding = buildBinding("REST", false, false);
        OrchestratorRestResourceRenderer restRenderer = new OrchestratorRestResourceRenderer();
        restRenderer.render(restBinding, new GenerationContext(
            processingEnv, tempDir, DeploymentRole.REST_SERVER, java.util.Set.of(), null, null));
        String restSource = Files.readString(tempDir.resolve("com/example/orchestrator/service/PipelineRunResource.java"));

        OrchestratorBinding grpcBinding = buildBinding("GRPC", false, false);
        OrchestratorGrpcRenderer grpcRenderer = new OrchestratorGrpcRenderer();
        grpcRenderer.render(grpcBinding, new GenerationContext(
            processingEnv,
            tempDir,
            DeploymentRole.PIPELINE_SERVER,
            java.util.Set.of(),
            null,
            buildGrpcDescriptorSet(false, false)));
        String grpcSource = Files.readString(tempDir.resolve("com/example/orchestrator/service/OrchestratorGrpcService.java"));

        OrchestratorBinding functionBinding = buildBinding("REST", false, false);
        OrchestratorFunctionHandlerRenderer functionRenderer = new OrchestratorFunctionHandlerRenderer();
        functionRenderer.render(functionBinding, new GenerationContext(
            processingEnv, tempDir, DeploymentRole.REST_SERVER, java.util.Set.of(), null, null));
        String functionRunAsync = Files.readString(
            tempDir.resolve("com/example/orchestrator/service/PipelineRunAsyncFunctionHandler.java"));
        String functionStatus = Files.readString(
            tempDir.resolve("com/example/orchestrator/service/PipelineExecutionStatusFunctionHandler.java"));
        String functionResult = Files.readString(
            tempDir.resolve("com/example/orchestrator/service/PipelineExecutionResultFunctionHandler.java"));

        assertTrue(restSource.contains("@Path(\"/run-async\")"));
        assertTrue(restSource.contains("@Path(\"/executions/{executionId}\")"));
        assertTrue(restSource.contains("@Path(\"/executions/{executionId}/result\")"));
        assertTrue(restSource.contains("executePipelineAsync"));

        assertTrue(grpcSource.contains("public Uni<RunAsyncResponse> runAsync(RunAsyncRequest request)"));
        assertTrue(grpcSource.contains("public Uni<GetExecutionStatusResponse> getExecutionStatus(GetExecutionStatusRequest request)"));
        assertTrue(grpcSource.contains("public Uni<GetExecutionResultResponse> getExecutionResult(GetExecutionResultRequest request)"));
        assertTrue(grpcSource.contains("executePipelineAsync"));

        assertTrue(functionRunAsync.contains("implements RequestHandler<PipelineRunAsyncRequest, RunAsyncAcceptedDto>"));
        assertTrue(functionRunAsync.contains("executePipelineAsync("));
        assertTrue(functionStatus.contains("throw new IllegalArgumentException(\"executionId is required\")"));
        assertTrue(functionResult.contains("throw new IllegalArgumentException(\"executionId is required\")"));
    }

    @Test
    void rendersStreamingAsyncResultContractAcrossGrpcAndFunction() throws IOException {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        Messager messager = mock(Messager.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));
        when(processingEnv.getMessager()).thenReturn(messager);

        OrchestratorBinding grpcBinding = buildBinding("GRPC", false, true);
        new OrchestratorGrpcRenderer().render(grpcBinding, new GenerationContext(
            processingEnv,
            tempDir,
            DeploymentRole.PIPELINE_SERVER,
            java.util.Set.of(),
            null,
            buildGrpcDescriptorSet(false, true)));
        String grpcSource = Files.readString(tempDir.resolve("com/example/orchestrator/service/OrchestratorGrpcService.java"));

        OrchestratorBinding functionBinding = buildBinding("REST", false, true);
        new OrchestratorFunctionHandlerRenderer().render(functionBinding, new GenerationContext(
            processingEnv, tempDir, DeploymentRole.REST_SERVER, java.util.Set.of(), null, null));
        String functionResult = Files.readString(
            tempDir.resolve("com/example/orchestrator/service/PipelineExecutionResultFunctionHandler.java"));

        assertTrue(grpcSource.contains("pipelineExecutionService.<List<OutputType>>getExecutionResult"));
        assertTrue(functionResult.contains("pipelineExecutionService.<List<OutputTypeDto>>getExecutionResult"));
    }

    private OrchestratorBinding buildBinding(String transport, boolean inputStreaming, boolean outputStreaming) {
        PipelineStepModel model = new PipelineStepModel(
            "OrchestratorService",
            "OrchestratorService",
            "com.example.orchestrator.service",
            ClassName.get("com.example.orchestrator.service", "OrchestratorService"),
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
            transport,
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

    private static StreamingShape streamingShape(boolean inputStreaming, boolean outputStreaming) {
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

    private static DescriptorProtos.FileDescriptorSet buildGrpcDescriptorSet(boolean inputStreaming, boolean outputStreaming) {
        DescriptorProtos.FileDescriptorProto proto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("orchestrator.proto")
            .setPackage("com.example.grpc")
            .setOptions(DescriptorProtos.FileOptions.newBuilder()
                .setJavaPackage("com.example.grpc")
                .setJavaOuterClassname("MutinyOrchestratorServiceGrpc")
                .setJavaMultipleFiles(true)
                .build())
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("InputType"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("OutputType"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("RunAsyncRequest"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("RunAsyncResponse"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("GetExecutionStatusRequest"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("GetExecutionStatusResponse"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("GetExecutionResultRequest"))
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
                    .setName("Ingest")
                    .setInputType(".com.example.grpc.InputType")
                    .setOutputType(".com.example.grpc.OutputType")
                    .setClientStreaming(true)
                    .setServerStreaming(true))
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Subscribe")
                    .setInputType(".com.example.grpc.InputType")
                    .setOutputType(".com.example.grpc.OutputType")
                    .setServerStreaming(true)))
            .build();
        return DescriptorProtos.FileDescriptorSet.newBuilder().addFile(proto).build();
    }
}
