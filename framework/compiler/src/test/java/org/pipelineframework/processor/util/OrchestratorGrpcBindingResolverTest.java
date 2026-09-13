package org.pipelineframework.processor.util;

import java.util.List;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.processor.ir.*;

import static org.junit.jupiter.api.Assertions.*;

class OrchestratorGrpcBindingResolverTest {

    @Test
    void resolvesUnaryBinding() {
        OrchestratorGrpcBindingResolver resolver = new OrchestratorGrpcBindingResolver();
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(false, false);
        PipelineStepModel model = buildModel();

        GrpcBinding binding = resolver.resolve(
            model,
            descriptorSet,
            OrchestratorRpcConstants.RUN_METHOD,
            false,
            false,
            null);

        assertNotNull(binding);
        assertEquals("OrchestratorService", serviceDescriptor(binding).getName());
        assertEquals("Run", methodDescriptor(binding).getName());
        assertFalse(methodDescriptor(binding).isClientStreaming());
        assertFalse(methodDescriptor(binding).isServerStreaming());
    }

    @Test
    void resolvesClientStreamingBinding() {
        OrchestratorGrpcBindingResolver resolver = new OrchestratorGrpcBindingResolver();
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(true, false);
        PipelineStepModel model = buildModel();

        GrpcBinding binding = resolver.resolve(
            model,
            descriptorSet,
            OrchestratorRpcConstants.RUN_METHOD,
            true,
            false,
            null);

        assertNotNull(binding);
        assertTrue(methodDescriptor(binding).isClientStreaming());
        assertFalse(methodDescriptor(binding).isServerStreaming());
    }

    @Test
    void resolvesServerStreamingBinding() {
        OrchestratorGrpcBindingResolver resolver = new OrchestratorGrpcBindingResolver();
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(false, true);
        PipelineStepModel model = buildModel();

        GrpcBinding binding = resolver.resolve(
            model,
            descriptorSet,
            OrchestratorRpcConstants.RUN_METHOD,
            false,
            true,
            null);

        assertNotNull(binding);
        assertFalse(methodDescriptor(binding).isClientStreaming());
        assertTrue(methodDescriptor(binding).isServerStreaming());
    }

    @Test
    void resolvesBidirectionalStreamingBinding() {
        OrchestratorGrpcBindingResolver resolver = new OrchestratorGrpcBindingResolver();
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(true, true);
        PipelineStepModel model = buildModel();

        GrpcBinding binding = resolver.resolve(
            model,
            descriptorSet,
            OrchestratorRpcConstants.RUN_METHOD,
            true,
            true,
            null);

        assertNotNull(binding);
        assertTrue(methodDescriptor(binding).isClientStreaming());
        assertTrue(methodDescriptor(binding).isServerStreaming());
    }

    @Test
    void resolvesRunAsyncMethod() {
        OrchestratorGrpcBindingResolver resolver = new OrchestratorGrpcBindingResolver();
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(false, false);
        PipelineStepModel model = buildModel();

        GrpcBinding binding = resolver.resolve(
            model,
            descriptorSet,
            OrchestratorRpcConstants.RUN_ASYNC_METHOD,
            false,
            false,
            null);

        assertNotNull(binding);
        assertEquals("RunAsync", methodDescriptor(binding).getName());
        assertFalse(methodDescriptor(binding).isClientStreaming());
        assertFalse(methodDescriptor(binding).isServerStreaming());
    }

    @Test
    void resolvesGetExecutionStatusMethod() {
        OrchestratorGrpcBindingResolver resolver = new OrchestratorGrpcBindingResolver();
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(false, false);
        PipelineStepModel model = buildModel();

        GrpcBinding binding = resolver.resolve(
            model,
            descriptorSet,
            OrchestratorRpcConstants.GET_EXECUTION_STATUS_METHOD,
            false,
            false,
            null);

        assertNotNull(binding);
        assertEquals("GetExecutionStatus", methodDescriptor(binding).getName());
    }

    @Test
    void resolvesGetExecutionResultMethod() {
        OrchestratorGrpcBindingResolver resolver = new OrchestratorGrpcBindingResolver();
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(false, false);
        PipelineStepModel model = buildModel();

        GrpcBinding binding = resolver.resolve(
            model,
            descriptorSet,
            OrchestratorRpcConstants.GET_EXECUTION_RESULT_METHOD,
            false,
            false,
            null);

        assertNotNull(binding);
        assertEquals("GetExecutionResult", methodDescriptor(binding).getName());
    }

    @Test
    void throwsExceptionWhenDescriptorSetIsNull() {
        OrchestratorGrpcBindingResolver resolver = new OrchestratorGrpcBindingResolver();
        PipelineStepModel model = buildModel();

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            resolver.resolve(model, null, OrchestratorRpcConstants.RUN_METHOD, false, false, null));

        assertTrue(exception.getMessage().contains("FileDescriptorSet is null"));
    }

    @Test
    void throwsExceptionWhenServiceNotFound() {
        OrchestratorGrpcBindingResolver resolver = new OrchestratorGrpcBindingResolver();
        DescriptorProtos.FileDescriptorSet descriptorSet = buildEmptyDescriptorSet();
        PipelineStepModel model = buildModel();

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            resolver.resolve(model, descriptorSet, OrchestratorRpcConstants.RUN_METHOD, false, false, null));

        assertTrue(exception.getMessage().contains("Service named 'OrchestratorService' not found"));
    }

    @Test
    void throwsExceptionWhenMethodNotFound() {
        OrchestratorGrpcBindingResolver resolver = new OrchestratorGrpcBindingResolver();
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(false, false);
        PipelineStepModel model = buildModel();

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            resolver.resolve(model, descriptorSet, "NonExistentMethod", false, false, null));

        assertTrue(exception.getMessage().contains("Method 'NonExistentMethod' not found"));
    }

    @Test
    void throwsExceptionWhenStreamingSemanticsMismatch() {
        OrchestratorGrpcBindingResolver resolver = new OrchestratorGrpcBindingResolver();
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(false, false);
        PipelineStepModel model = buildModel();

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            resolver.resolve(
                model,
                descriptorSet,
                OrchestratorRpcConstants.RUN_METHOD,
                true,
                false,
                null));

        assertTrue(exception.getMessage().contains("streaming semantics mismatch"));
        assertTrue(exception.getMessage().contains("clientStreaming expected=true actual=false"));
        assertTrue(exception.getMessage().contains("serverStreaming expected=false actual=false"));
    }

    @Test
    void handlesDescriptorSetWithDependencies() {
        OrchestratorGrpcBindingResolver resolver = new OrchestratorGrpcBindingResolver();
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSetWithDependencies();
        PipelineStepModel model = buildModel();

        GrpcBinding binding = resolver.resolve(
            model,
            descriptorSet,
            OrchestratorRpcConstants.RUN_METHOD,
            false,
            false,
            null);

        assertNotNull(binding);
        assertEquals("OrchestratorService", serviceDescriptor(binding).getName());
    }

    @Test
    void resolvesIngestMethod() {
        OrchestratorGrpcBindingResolver resolver = new OrchestratorGrpcBindingResolver();
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(false, false);
        PipelineStepModel model = buildModel();

        GrpcBinding binding = resolver.resolve(
            model,
            descriptorSet,
            OrchestratorRpcConstants.INGEST_METHOD,
            true,
            true,
            null);

        assertNotNull(binding);
        assertEquals("Ingest", methodDescriptor(binding).getName());
        assertTrue(methodDescriptor(binding).isClientStreaming());
        assertTrue(methodDescriptor(binding).isServerStreaming());
    }

    @Test
    void resolvesSubscribeMethod() {
        OrchestratorGrpcBindingResolver resolver = new OrchestratorGrpcBindingResolver();
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(false, false);
        PipelineStepModel model = buildModel();

        GrpcBinding binding = resolver.resolve(
            model,
            descriptorSet,
            OrchestratorRpcConstants.SUBSCRIBE_METHOD,
            false,
            true,
            null);

        assertNotNull(binding);
        assertEquals("Subscribe", methodDescriptor(binding).getName());
        assertFalse(methodDescriptor(binding).isClientStreaming());
        assertTrue(methodDescriptor(binding).isServerStreaming());
    }

    private PipelineStepModel buildModel() {
        return new PipelineStepModel(
            "OrchestratorService",
            "OrchestratorService",
            "com.example.orchestrator.service",
            ClassName.get("com.example.orchestrator.service", "OrchestratorService"),
            null,
            null,
            StreamingShape.UNARY_UNARY,
            java.util.Set.of(GenerationTarget.GRPC_SERVICE),
            ExecutionMode.DEFAULT,
            DeploymentRole.ORCHESTRATOR_CLIENT,
            false,
            null
        );
    }

    private static Descriptors.ServiceDescriptor serviceDescriptor(GrpcBinding binding) {
        return (Descriptors.ServiceDescriptor) binding.serviceDescriptor();
    }

    private static Descriptors.MethodDescriptor methodDescriptor(GrpcBinding binding) {
        return (Descriptors.MethodDescriptor) binding.methodDescriptor();
    }

    private DescriptorProtos.FileDescriptorSet buildDescriptorSet(
        boolean inputStreaming,
        boolean outputStreaming
    ) {
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

    private DescriptorProtos.FileDescriptorSet buildEmptyDescriptorSet() {
        DescriptorProtos.FileDescriptorProto proto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("empty.proto")
            .setPackage("com.example.grpc")
            .build();

        return DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(proto)
            .build();
    }

    private DescriptorProtos.FileDescriptorSet buildDescriptorSetWithDependencies() {
        DescriptorProtos.FileDescriptorProto baseProto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("base.proto")
            .setPackage("com.example.grpc.base")
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("BaseMessage"))
            .build();

        DescriptorProtos.FileDescriptorProto orchestratorProto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("orchestrator.proto")
            .setPackage("com.example.grpc")
            .addDependency("base.proto")
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
                    .setOutputType(".com.example.grpc.OutputType")))
            .build();

        return DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(baseProto)
            .addFile(orchestratorProto)
            .build();
    }
}
