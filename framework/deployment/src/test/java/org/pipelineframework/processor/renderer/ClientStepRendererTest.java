package org.pipelineframework.processor.renderer;

import java.nio.file.Path;
import javax.annotation.processing.ProcessingEnvironment;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.*;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClientStepRendererTest {

    private ClientStepRenderer renderer;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        renderer = new ClientStepRenderer(org.pipelineframework.processor.ir.GenerationTarget.CLIENT_STEP);
    }

    @Test
    void testRenderUnaryUnaryClientStep() {
        PipelineStepModel model = createModel(StreamingShape.UNARY_UNARY, false);

        // Build real descriptors to avoid mocking final protobuf types
        Descriptors.FileDescriptor fileDescriptor = buildFileDescriptor();
        Descriptors.ServiceDescriptor serviceDescriptor = fileDescriptor.findServiceByName("TestService");
        Descriptors.MethodDescriptor methodDescriptor = serviceDescriptor.findMethodByName("remoteProcess");
        GrpcBinding binding = new GrpcBinding(model, serviceDescriptor, methodDescriptor);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getElementUtils()).thenReturn(null);
        when(processingEnv.getTypeUtils()).thenReturn(null);
        when(processingEnv.getFiler()).thenReturn(null);
        when(processingEnv.getMessager()).thenReturn(null);

        // Create a mock context for the renderer
        var context = new GenerationContext(processingEnv, tempDir, DeploymentRole.ORCHESTRATOR_CLIENT,
            java.util.Set.of(), null, null);

        assertDoesNotThrow(() -> renderer.render(binding, context));

        Path clientStep = tempDir.resolve("com/example/pipeline/TestGrpcClientStep.java");
        String source = assertDoesNotThrow(() -> java.nio.file.Files.readString(clientStep));
        assertTrue(source.contains("implements TransportBoundaryInvocation"));
        assertTrue(source.contains("public TransportBoundaryDescriptor transportBoundary()"));
        assertTrue(source.contains("PipelineInvocationRuntime invocationRuntime;"));
        assertTrue(source.contains("this.invocationRuntime.invokeTransportUni"));
        assertTrue(source.contains("new TransportBoundaryDescriptor(\"grpc\", \"TestService.remoteProcess\")"));
    }

    @Test
    void testRenderUnaryStreamingClientStep() {
        PipelineStepModel model = createModel(StreamingShape.UNARY_STREAMING, false);

        // Build real descriptors to avoid mocking final protobuf types
        Descriptors.FileDescriptor fileDescriptor = buildFileDescriptor();
        Descriptors.ServiceDescriptor serviceDescriptor = fileDescriptor.findServiceByName("TestService");
        Descriptors.MethodDescriptor methodDescriptor = serviceDescriptor.findMethodByName("remoteProcess");
        GrpcBinding binding = new GrpcBinding(model, serviceDescriptor, methodDescriptor);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getElementUtils()).thenReturn(null);
        when(processingEnv.getTypeUtils()).thenReturn(null);
        when(processingEnv.getFiler()).thenReturn(null);
        when(processingEnv.getMessager()).thenReturn(null);

        // Create a mock context for the renderer
        var context = new GenerationContext(processingEnv, tempDir, DeploymentRole.ORCHESTRATOR_CLIENT,
            java.util.Set.of(), null, null);

        assertDoesNotThrow(() -> renderer.render(binding, context));

        Path clientStep = tempDir.resolve("com/example/pipeline/TestGrpcClientStep.java");
        String source = assertDoesNotThrow(() -> java.nio.file.Files.readString(clientStep));
        assertTrue(source.contains("this.invocationRuntime.invokeTransportMulti"));
    }

    @Test
    void rendersSideEffectClientStepWithCacheReadBypass() throws Exception {
        PipelineStepModel model = createModel(StreamingShape.UNARY_UNARY, true);

        Descriptors.FileDescriptor fileDescriptor = buildFileDescriptor();
        Descriptors.ServiceDescriptor serviceDescriptor = fileDescriptor.findServiceByName("TestService");
        Descriptors.MethodDescriptor methodDescriptor = serviceDescriptor.findMethodByName("remoteProcess");
        GrpcBinding binding = new GrpcBinding(model, serviceDescriptor, methodDescriptor);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getElementUtils()).thenReturn(null);
        when(processingEnv.getTypeUtils()).thenReturn(null);
        when(processingEnv.getFiler()).thenReturn(null);
        when(processingEnv.getMessager()).thenReturn(null);

        var context = new GenerationContext(processingEnv, tempDir, DeploymentRole.ORCHESTRATOR_CLIENT,
            java.util.Set.of(), null, null);

        renderer.render(binding, context);

        Path clientStep = tempDir.resolve("com/example/pipeline/TestGrpcClientStep.java");
        String source = java.nio.file.Files.readString(clientStep);
        org.junit.jupiter.api.Assertions.assertTrue(source.contains("CacheReadBypass"));
    }

    @Test
    void rendersAllV3GrpcCardinalitiesWithCanonicalContractsAndProtobufTransport() throws Exception {
        for (StreamingShape shape : StreamingShape.values()) {
            PipelineStepModel model = createModel(shape, false);
            Descriptors.FileDescriptor fileDescriptor = buildFileDescriptor(
                shape == StreamingShape.STREAMING_UNARY || shape == StreamingShape.STREAMING_STREAMING,
                shape == StreamingShape.UNARY_STREAMING || shape == StreamingShape.STREAMING_STREAMING);
            GrpcBinding binding = new GrpcBinding(
                model,
                fileDescriptor.findServiceByName("TestService"),
                fileDescriptor.findServiceByName("TestService").findMethodByName("remoteProcess"));
            ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
            when(processingEnv.getElementUtils()).thenReturn(null);
            when(processingEnv.getTypeUtils()).thenReturn(null);
            when(processingEnv.getFiler()).thenReturn(null);
            when(processingEnv.getMessager()).thenReturn(null);
            var context = new GenerationContext(
                processingEnv,
                tempDir,
                DeploymentRole.ORCHESTRATOR_CLIENT,
                java.util.Set.of(),
                null,
                null,
                PipelineTransport.GRPC,
                "com.example",
                null,
                true);

            renderer.render(binding, context);

            String source = java.nio.file.Files.readString(
                tempDir.resolve("com/example/pipeline/TestGrpcClientStep.java"));
            assertTrue(source.contains(v3StepContract(shape)));
            assertTrue(source.contains("PipelineTypes.MutinyTestServiceStub"));
            assertTrue(source.contains("PipelineDomainProtoAdapters.toProto"));
            assertTrue(source.contains("PipelineDomainProtoAdapters.fromProto"));
            assertTrue(!source.contains("Mapper<"));
        }
    }

    private static String v3StepContract(StreamingShape shape) {
        return switch (shape) {
            case UNARY_UNARY -> "StepOneToOne<InputType, OutputType>";
            case UNARY_STREAMING -> "StepOneToMany<InputType, OutputType>";
            case STREAMING_UNARY -> "StepManyToOne<InputType, OutputType>";
            case STREAMING_STREAMING -> "StepManyToMany<InputType, OutputType>";
        };
    }

    private TypeMapping createTypeMapping(String simpleName) {
        return new TypeMapping(
            ClassName.get("com.example.domain", simpleName),
            null,  // mapperType - can be null for this test
            false  // hasMapper
        );
    }

    private PipelineStepModel createModel(StreamingShape shape, boolean sideEffect) {
        return new PipelineStepModel.Builder()
            .serviceName("TestService")
            .servicePackage("com.example")
            .serviceClassName(ClassName.get("com.example", "TestService"))
            .inputMapping(createTypeMapping("InputType"))
            .outputMapping(createTypeMapping("OutputType"))
            .streamingShape(shape)
            .executionMode(ExecutionMode.DEFAULT)
            .sideEffect(sideEffect)
            .enabledTargets(java.util.Set.of(GenerationTarget.CLIENT_STEP))
            .build();
    }

    private Descriptors.FileDescriptor buildFileDescriptor() {
        return buildFileDescriptor(false, false);
    }

    private Descriptors.FileDescriptor buildFileDescriptor(boolean clientStreaming, boolean serverStreaming) {
        DescriptorProtos.FileDescriptorProto proto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("test_service.proto")
            .setPackage("com.example.grpc")
            .setOptions(DescriptorProtos.FileOptions.newBuilder()
                .setJavaPackage("com.example.grpc")
                .setJavaOuterClassname("PipelineTypes")
                .build())
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                .setName("InputType"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                .setName("OutputType"))
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("TestService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("remoteProcess")
                    .setInputType(".com.example.grpc.InputType")
                    .setOutputType(".com.example.grpc.OutputType")
                    .setClientStreaming(clientStreaming)
                    .setServerStreaming(serverStreaming)))
            .build();

        try {
            return Descriptors.FileDescriptor.buildFrom(proto, new Descriptors.FileDescriptor[] {});
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException("Failed to build test descriptor", e);
        }
    }

}
