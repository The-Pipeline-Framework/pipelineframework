package org.pipelineframework.processor.util;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.DeploymentRole;

import javax.annotation.processing.Messager;
import javax.tools.Diagnostic;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OrchestratorGrpcBindingResolverTest {

    private OrchestratorGrpcBindingResolver resolver;
    private Messager messager;

    @BeforeEach
    void setUp() {
        resolver = new OrchestratorGrpcBindingResolver();
        messager = mock(Messager.class);
    }

    @Test
    void resolvesUnaryOrchestrat orRpcBinding() {
        PipelineStepModel model = createModel("OrchestratorService");
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(
            "OrchestratorService", "Run", false, false);

        GrpcBinding binding = resolver.resolve(model, descriptorSet, "Run", false, false, messager);

        assertNotNull(binding);
        assertEquals("OrchestratorService", binding.serviceDescriptor().getName());
        assertEquals("Run", binding.methodDescriptor().getName());
        assertFalse(binding.methodDescriptor().isClientStreaming());
        assertFalse(binding.methodDescriptor().isServerStreaming());
    }

    @Test
    void resolvesStreamingInputRpcBinding() {
        PipelineStepModel model = createModel("OrchestratorService");
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(
            "OrchestratorService", "Run", true, false);

        GrpcBinding binding = resolver.resolve(model, descriptorSet, "Run", true, false, messager);

        assertNotNull(binding);
        assertTrue(binding.methodDescriptor().isClientStreaming());
        assertFalse(binding.methodDescriptor().isServerStreaming());
    }

    @Test
    void resolvesStreamingOutputRpcBinding() {
        PipelineStepModel model = createModel("OrchestratorService");
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(
            "OrchestratorService", "Run", false, true);

        GrpcBinding binding = resolver.resolve(model, descriptorSet, "Run", false, true, messager);

        assertNotNull(binding);
        assertFalse(binding.methodDescriptor().isClientStreaming());
        assertTrue(binding.methodDescriptor().isServerStreaming());
    }

    @Test
    void resolvesBidirectionalStreamingRpcBinding() {
        PipelineStepModel model = createModel("OrchestratorService");
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(
            "OrchestratorService", "Run", true, true);

        GrpcBinding binding = resolver.resolve(model, descriptorSet, "Run", true, true, messager);

        assertNotNull(binding);
        assertTrue(binding.methodDescriptor().isClientStreaming());
        assertTrue(binding.methodDescriptor().isServerStreaming());
    }

    @Test
    void resolvesRunAsyncMethod() {
        PipelineStepModel model = createModel("OrchestratorService");
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSetWithMultipleMethods();

        GrpcBinding binding = resolver.resolve(model, descriptorSet, "RunAsync", false, false, messager);

        assertNotNull(binding);
        assertEquals("RunAsync", binding.methodDescriptor().getName());
    }

    @Test
    void resolvesGetExecutionStatusMethod() {
        PipelineStepModel model = createModel("OrchestratorService");
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSetWithMultipleMethods();

        GrpcBinding binding = resolver.resolve(model, descriptorSet, "GetExecutionStatus", false, false, messager);

        assertNotNull(binding);
        assertEquals("GetExecutionStatus", binding.methodDescriptor().getName());
    }

    @Test
    void resolvesGetExecutionResultMethod() {
        PipelineStepModel model = createModel("OrchestratorService");
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSetWithMultipleMethods();

        GrpcBinding binding = resolver.resolve(model, descriptorSet, "GetExecutionResult", false, false, messager);

        assertNotNull(binding);
        assertEquals("GetExecutionResult", binding.methodDescriptor().getName());
    }

    @Test
    void resolvesIngestMethod() {
        PipelineStepModel model = createModel("OrchestratorService");
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSetWithMultipleMethods();

        GrpcBinding binding = resolver.resolve(model, descriptorSet, "Ingest", true, true, messager);

        assertNotNull(binding);
        assertEquals("Ingest", binding.methodDescriptor().getName());
    }

    @Test
    void resolvesSubscribeMethod() {
        PipelineStepModel model = createModel("OrchestratorService");
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSetWithMultipleMethods();

        GrpcBinding binding = resolver.resolve(model, descriptorSet, "Subscribe", false, true, messager);

        assertNotNull(binding);
        assertEquals("Subscribe", binding.methodDescriptor().getName());
    }

    @Test
    void throwsExceptionWhenDescriptorSetIsNull() {
        PipelineStepModel model = createModel("OrchestratorService");

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> resolver.resolve(model, null, "Run", false, false, messager));

        assertTrue(exception.getMessage().contains("FileDescriptorSet is null"));
        assertTrue(exception.getMessage().contains("protobuf compilation"));
    }

    @Test
    void throwsExceptionWhenServiceNotFound() {
        PipelineStepModel model = createModel("NonExistentService");
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(
            "OrchestratorService", "Run", false, false);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> resolver.resolve(model, descriptorSet, "Run", false, false, messager));

        assertTrue(exception.getMessage().contains("Service named 'NonExistentService' not found"));
    }

    @Test
    void throwsExceptionWhenMethodNotFound() {
        PipelineStepModel model = createModel("OrchestratorService");
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(
            "OrchestratorService", "Run", false, false);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> resolver.resolve(model, descriptorSet, "NonExistentMethod", false, false, messager));

        assertTrue(exception.getMessage().contains("Method 'NonExistentMethod' not found"));
    }

    @Test
    void throwsExceptionWhenStreamingSemanticsMismatch() {
        PipelineStepModel model = createModel("OrchestratorService");
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(
            "OrchestratorService", "Run", false, false);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> resolver.resolve(model, descriptorSet, "Run", true, false, messager));

        assertTrue(exception.getMessage().contains("streaming semantics do not match"));
    }

    @Test
    void warnsWhenMultipleRpcsFoundWithUnexpectedMethods() {
        PipelineStepModel model = createModel("OrchestratorService");
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSetWithUnexpectedMethod();

        resolver.resolve(model, descriptorSet, "Run", false, false, messager);

        verify(messager).printMessage(any(Diagnostic.Kind.class), anyString());
    }

    @Test
    void handlesFileDescriptorDependencies() {
        PipelineStepModel model = createModel("OrchestratorService");
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSetWithDependencies();

        GrpcBinding binding = resolver.resolve(model, descriptorSet, "Run", false, false, messager);

        assertNotNull(binding);
        assertEquals("OrchestratorService", binding.serviceDescriptor().getName());
    }

    @Test
    void throwsExceptionWhenCircularDependencyDetected() {
        PipelineStepModel model = createModel("OrchestratorService");
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSetWithCircularDependency();

        assertThrows(IllegalStateException.class,
            () -> resolver.resolve(model, descriptorSet, "Run", false, false, messager));
    }

    @Test
    void throwsExceptionWhenDuplicateServiceNames() {
        PipelineStepModel model = createModel("OrchestratorService");
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSetWithDuplicateServices();

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> resolver.resolve(model, descriptorSet, "Run", false, false, messager));

        assertTrue(exception.getMessage().contains("Multiple services named 'OrchestratorService'"));
    }

    @Test
    void throwsExceptionWhenDuplicateMethodNames() {
        PipelineStepModel model = createModel("OrchestratorService");
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSetWithDuplicateMethods();

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> resolver.resolve(model, descriptorSet, "Run", false, false, messager));

        assertTrue(exception.getMessage().contains("Multiple methods named 'Run'"));
    }

    private PipelineStepModel createModel(String serviceName) {
        return new PipelineStepModel(
            serviceName,
            serviceName,
            "com.example.orchestrator.service",
            com.squareup.javapoet.ClassName.get("com.example.orchestrator.service", serviceName),
            null,
            null,
            StreamingShape.UNARY_UNARY,
            java.util.Set.of(GenerationTarget.GRPC_SERVICE),
            ExecutionMode.DEFAULT,
            DeploymentRole.PIPELINE_SERVER,
            false,
            null
        );
    }

    private DescriptorProtos.FileDescriptorSet buildDescriptorSet(
            String serviceName, String methodName, boolean inputStreaming, boolean outputStreaming) {
        DescriptorProtos.FileDescriptorProto proto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("orchestrator.proto")
            .setPackage("com.example.grpc")
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("InputType"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("OutputType"))
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName(serviceName)
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName(methodName)
                    .setInputType(".com.example.grpc.InputType")
                    .setOutputType(".com.example.grpc.OutputType")
                    .setClientStreaming(inputStreaming)
                    .setServerStreaming(outputStreaming)))
            .build();

        return DescriptorProtos.FileDescriptorSet.newBuilder().addFile(proto).build();
    }

    private DescriptorProtos.FileDescriptorSet buildDescriptorSetWithMultipleMethods() {
        DescriptorProtos.FileDescriptorProto proto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("orchestrator.proto")
            .setPackage("com.example.grpc")
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("InputType"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("OutputType"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("RunAsyncRequest"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("RunAsyncResponse"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("GetExecutionStatusRequest"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("GetExecutionStatusResponse"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("GetExecutionResultRequest"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("GetExecutionResultResponse"))
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
                    .setServerStreaming(true)))
            .build();

        return DescriptorProtos.FileDescriptorSet.newBuilder().addFile(proto).build();
    }

    private DescriptorProtos.FileDescriptorSet buildDescriptorSetWithUnexpectedMethod() {
        DescriptorProtos.FileDescriptorProto proto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("orchestrator.proto")
            .setPackage("com.example.grpc")
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("InputType"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("OutputType"))
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("OrchestratorService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Run")
                    .setInputType(".com.example.grpc.InputType")
                    .setOutputType(".com.example.grpc.OutputType"))
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("UnexpectedMethod")
                    .setInputType(".com.example.grpc.InputType")
                    .setOutputType(".com.example.grpc.OutputType")))
            .build();

        return DescriptorProtos.FileDescriptorSet.newBuilder().addFile(proto).build();
    }

    private DescriptorProtos.FileDescriptorSet buildDescriptorSetWithDependencies() {
        DescriptorProtos.FileDescriptorProto baseProto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("base.proto")
            .setPackage("com.example.grpc")
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("InputType"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("OutputType"))
            .build();

        DescriptorProtos.FileDescriptorProto orchestratorProto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("orchestrator.proto")
            .setPackage("com.example.grpc")
            .addDependency("base.proto")
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("OrchestratorService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Run")
                    .setInputType(".com.example.grpc.InputType")
                    .setOutputType(".com.example.grpc.OutputType")))
            .build();

        return DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(baseProto)
            .addFile(orchestratorProto)
            .build();
    }

    private DescriptorProtos.FileDescriptorSet buildDescriptorSetWithCircularDependency() {
        DescriptorProtos.FileDescriptorProto proto1 = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("proto1.proto")
            .setPackage("com.example.grpc")
            .addDependency("proto2.proto")
            .build();

        DescriptorProtos.FileDescriptorProto proto2 = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("proto2.proto")
            .setPackage("com.example.grpc")
            .addDependency("proto1.proto")
            .build();

        return DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(proto1)
            .addFile(proto2)
            .build();
    }

    private DescriptorProtos.FileDescriptorSet buildDescriptorSetWithDuplicateServices() {
        DescriptorProtos.FileDescriptorProto proto1 = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("proto1.proto")
            .setPackage("com.example.grpc")
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("InputType"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("OutputType"))
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("OrchestratorService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Run")
                    .setInputType(".com.example.grpc.InputType")
                    .setOutputType(".com.example.grpc.OutputType")))
            .build();

        DescriptorProtos.FileDescriptorProto proto2 = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("proto2.proto")
            .setPackage("com.example.grpc")
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("OrchestratorService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Run")
                    .setInputType(".com.example.grpc.InputType")
                    .setOutputType(".com.example.grpc.OutputType")))
            .build();

        return DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(proto1)
            .addFile(proto2)
            .build();
    }

    private DescriptorProtos.FileDescriptorSet buildDescriptorSetWithDuplicateMethods() {
        DescriptorProtos.FileDescriptorProto proto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("orchestrator.proto")
            .setPackage("com.example.grpc")
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("InputType"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("OutputType"))
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("OrchestratorService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Run")
                    .setInputType(".com.example.grpc.InputType")
                    .setOutputType(".com.example.grpc.OutputType"))
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Run")
                    .setInputType(".com.example.grpc.InputType")
                    .setOutputType(".com.example.grpc.OutputType")))
            .build();

        return DescriptorProtos.FileDescriptorSet.newBuilder().addFile(proto).build();
    }
}