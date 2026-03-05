package org.pipelineframework.processor.util;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.processing.Messager;
import javax.tools.Diagnostic;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GrpcBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class OrchestratorGrpcBindingResolverTest {

    private OrchestratorGrpcBindingResolver resolver;
    private PipelineStepModel model;

    @BeforeEach
    void setUp() {
        resolver = new OrchestratorGrpcBindingResolver();
        model = new PipelineStepModel.Builder()
            .serviceName("OrchestratorService")
            .generatedName("OrchestratorService")
            .servicePackage("com.example.orchestrator")
            .serviceClassName(ClassName.get("com.example.orchestrator", "OrchestratorService"))
            .inputMapping(new org.pipelineframework.processor.ir.TypeMapping(
                ClassName.get("com.example", "Input"), null, false))
            .outputMapping(new org.pipelineframework.processor.ir.TypeMapping(
                ClassName.get("com.example", "Output"), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .sideEffect(false)
            .build();
    }

    @Test
    void resolvesUnaryUnaryBinding() {
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(false, false);

        GrpcBinding binding = resolver.resolve(
            model,
            descriptorSet,
            OrchestratorRpcConstants.RUN_METHOD,
            false,
            false,
            null);

        assertNotNull(binding);
        assertNotNull(binding.serviceDescriptor());
        assertNotNull(binding.methodDescriptor());
        assertEquals("OrchestratorService", binding.serviceDescriptor().getName());
        assertEquals("Run", binding.methodDescriptor().getName());
    }

    @Test
    void resolvesStreamingInputBinding() {
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(true, false);

        GrpcBinding binding = resolver.resolve(
            model,
            descriptorSet,
            OrchestratorRpcConstants.RUN_METHOD,
            true,
            false,
            null);

        assertNotNull(binding);
        assertTrue(binding.methodDescriptor().isClientStreaming());
        assertTrue(!binding.methodDescriptor().isServerStreaming());
    }

    @Test
    void resolvesStreamingOutputBinding() {
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(false, true);

        GrpcBinding binding = resolver.resolve(
            model,
            descriptorSet,
            OrchestratorRpcConstants.RUN_METHOD,
            false,
            true,
            null);

        assertNotNull(binding);
        assertTrue(!binding.methodDescriptor().isClientStreaming());
        assertTrue(binding.methodDescriptor().isServerStreaming());
    }

    @Test
    void resolvesBidirectionalStreamingBinding() {
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(true, true);

        GrpcBinding binding = resolver.resolve(
            model,
            descriptorSet,
            OrchestratorRpcConstants.INGEST_METHOD,
            true,
            true,
            null);

        assertNotNull(binding);
        assertTrue(binding.methodDescriptor().isClientStreaming());
        assertTrue(binding.methodDescriptor().isServerStreaming());
    }

    @Test
    void resolvesAsyncRpcMethods() {
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSetWithAsyncMethods();

        GrpcBinding runAsyncBinding = resolver.resolve(
            model,
            descriptorSet,
            OrchestratorRpcConstants.RUN_ASYNC_METHOD,
            false,
            false,
            null);
        assertEquals("RunAsync", runAsyncBinding.methodDescriptor().getName());

        GrpcBinding statusBinding = resolver.resolve(
            model,
            descriptorSet,
            OrchestratorRpcConstants.GET_EXECUTION_STATUS_METHOD,
            false,
            false,
            null);
        assertEquals("GetExecutionStatus", statusBinding.methodDescriptor().getName());

        GrpcBinding resultBinding = resolver.resolve(
            model,
            descriptorSet,
            OrchestratorRpcConstants.GET_EXECUTION_RESULT_METHOD,
            false,
            false,
            null);
        assertEquals("GetExecutionResult", resultBinding.methodDescriptor().getName());
    }

    @Test
    void throwsWhenDescriptorSetIsNull() {
        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            resolver.resolve(model, null, OrchestratorRpcConstants.RUN_METHOD, false, false, null));

        assertTrue(exception.getMessage().contains("FileDescriptorSet is null"));
        assertTrue(exception.getMessage().contains("protobuf compilation"));
    }

    @Test
    void throwsWhenServiceNotFound() {
        DescriptorProtos.FileDescriptorSet emptyDescriptorSet =
            DescriptorProtos.FileDescriptorSet.newBuilder()
                .addFile(DescriptorProtos.FileDescriptorProto.newBuilder()
                    .setName("empty.proto")
                    .setPackage("com.example")
                    .build())
                .build();

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            resolver.resolve(model, emptyDescriptorSet, OrchestratorRpcConstants.RUN_METHOD, false, false, null));

        assertTrue(exception.getMessage().contains("Service named 'OrchestratorService' not found"));
    }

    @Test
    void throwsWhenMethodNotFound() {
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(false, false);

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            resolver.resolve(model, descriptorSet, "NonExistentMethod", false, false, null));

        assertTrue(exception.getMessage().contains("Method 'NonExistentMethod' not found"));
    }

    @Test
    void throwsWhenStreamingSemanticsDoNotMatch() {
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSet(false, false);

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            resolver.resolve(model, descriptorSet, OrchestratorRpcConstants.RUN_METHOD, true, false, null));

        assertTrue(exception.getMessage().contains("streaming semantics do not match"));
    }

    @Test
    void warnsWhenMultipleUnexpectedRpcsFound() {
        Messager messager = mock(Messager.class);
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSetWithExtraMethod();

        resolver.resolve(model, descriptorSet, OrchestratorRpcConstants.RUN_METHOD, false, false, messager);

        verify(messager).printMessage(any(Diagnostic.Kind.class), contains("Multiple RPCs found"));
    }

    @Test
    void handlesDescriptorWithDependencies() {
        DescriptorProtos.FileDescriptorSet descriptorSet = buildDescriptorSetWithDependency();

        GrpcBinding binding = resolver.resolve(
            model,
            descriptorSet,
            OrchestratorRpcConstants.RUN_METHOD,
            false,
            false,
            null);

        assertNotNull(binding);
        assertNotNull(binding.serviceDescriptor());
    }

    @Test
    void throwsWhenFileDescriptorDependenciesCannotBeResolved() {
        DescriptorProtos.FileDescriptorProto dependentProto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("dependent.proto")
            .setPackage("com.example")
            .addDependency("missing.proto")
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("OrchestratorService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Run")
                    .setInputType(".com.example.Input")
                    .setOutputType(".com.example.Output")
                    .build())
                .build())
            .build();

        DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(dependentProto)
            .build();

        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
            resolver.resolve(model, descriptorSet, OrchestratorRpcConstants.RUN_METHOD, false, false, null));

        assertTrue(exception.getMessage().contains("Could not resolve all file descriptor dependencies")
            || exception.getMessage().contains("Unbuilt files"));
    }

    private DescriptorProtos.FileDescriptorSet buildDescriptorSet(boolean inputStreaming, boolean outputStreaming) {
        DescriptorProtos.FileDescriptorProto proto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("orchestrator.proto")
            .setPackage("com.example")
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Input").build())
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Output").build())
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("OrchestratorService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Run")
                    .setInputType(".com.example.Input")
                    .setOutputType(".com.example.Output")
                    .setClientStreaming(inputStreaming)
                    .setServerStreaming(outputStreaming)
                    .build())
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Ingest")
                    .setInputType(".com.example.Input")
                    .setOutputType(".com.example.Output")
                    .setClientStreaming(true)
                    .setServerStreaming(true)
                    .build())
                .build())
            .build();

        return DescriptorProtos.FileDescriptorSet.newBuilder().addFile(proto).build();
    }

    private DescriptorProtos.FileDescriptorSet buildDescriptorSetWithAsyncMethods() {
        DescriptorProtos.FileDescriptorProto proto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("orchestrator.proto")
            .setPackage("com.example")
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Input").build())
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Output").build())
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("RunAsyncRequest").build())
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("RunAsyncResponse").build())
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("GetExecutionStatusRequest").build())
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("GetExecutionStatusResponse").build())
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("GetExecutionResultRequest").build())
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("GetExecutionResultResponse").build())
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("OrchestratorService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Run")
                    .setInputType(".com.example.Input")
                    .setOutputType(".com.example.Output")
                    .build())
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("RunAsync")
                    .setInputType(".com.example.RunAsyncRequest")
                    .setOutputType(".com.example.RunAsyncResponse")
                    .build())
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("GetExecutionStatus")
                    .setInputType(".com.example.GetExecutionStatusRequest")
                    .setOutputType(".com.example.GetExecutionStatusResponse")
                    .build())
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("GetExecutionResult")
                    .setInputType(".com.example.GetExecutionResultRequest")
                    .setOutputType(".com.example.GetExecutionResultResponse")
                    .build())
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Ingest")
                    .setInputType(".com.example.Input")
                    .setOutputType(".com.example.Output")
                    .setClientStreaming(true)
                    .setServerStreaming(true)
                    .build())
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Subscribe")
                    .setInputType(".com.example.Input")
                    .setOutputType(".com.example.Output")
                    .setServerStreaming(true)
                    .build())
                .build())
            .build();

        return DescriptorProtos.FileDescriptorSet.newBuilder().addFile(proto).build();
    }

    private DescriptorProtos.FileDescriptorSet buildDescriptorSetWithExtraMethod() {
        DescriptorProtos.FileDescriptorProto proto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("orchestrator.proto")
            .setPackage("com.example")
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Input").build())
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Output").build())
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("OrchestratorService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Run")
                    .setInputType(".com.example.Input")
                    .setOutputType(".com.example.Output")
                    .build())
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("UnexpectedExtraMethod")
                    .setInputType(".com.example.Input")
                    .setOutputType(".com.example.Output")
                    .build())
                .build())
            .build();

        return DescriptorProtos.FileDescriptorSet.newBuilder().addFile(proto).build();
    }

    private DescriptorProtos.FileDescriptorSet buildDescriptorSetWithDependency() {
        DescriptorProtos.FileDescriptorProto baseProto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("base.proto")
            .setPackage("com.example")
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Input").build())
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Output").build())
            .build();

        DescriptorProtos.FileDescriptorProto orchestratorProto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("orchestrator.proto")
            .setPackage("com.example")
            .addDependency("base.proto")
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("OrchestratorService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("Run")
                    .setInputType(".com.example.Input")
                    .setOutputType(".com.example.Output")
                    .build())
                .build())
            .build();

        return DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(baseProto)
            .addFile(orchestratorProto)
            .build();
    }
}