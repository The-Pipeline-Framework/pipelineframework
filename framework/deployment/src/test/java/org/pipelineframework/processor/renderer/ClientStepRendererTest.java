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
        PipelineStepModel model = createModel(StreamingShape.UNARY_UNARY);

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

        // Render the client step - this should not throw an exception
        assertDoesNotThrow(() -> renderer.render(binding, context));
    }

    @Test
    void testRenderUnaryStreamingClientStep() {
        PipelineStepModel model = createModel(StreamingShape.UNARY_STREAMING);

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

        // Render the client step - this should not throw an exception
        assertDoesNotThrow(() -> renderer.render(binding, context));
    }

    private TypeMapping createTypeMapping(String simpleName) {
        return new TypeMapping(
            ClassName.get("com.example.domain", simpleName),
            null,  // mapperType - can be null for this test
            false  // hasMapper
        );
    }

    private PipelineStepModel createModel(StreamingShape shape) {
        return new PipelineStepModel.Builder()
            .serviceName("TestService")
            .servicePackage("com.example")
            .serviceClassName(ClassName.get("com.example", "TestService"))
            .inputMapping(createTypeMapping("InputType"))
            .outputMapping(createTypeMapping("OutputType"))
            .streamingShape(shape)
            .executionMode(ExecutionMode.DEFAULT)
            .enabledTargets(java.util.Set.of(GenerationTarget.CLIENT_STEP))
            .build();
    }

    private Descriptors.FileDescriptor buildFileDescriptor() {
        DescriptorProtos.FileDescriptorProto proto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("test_service.proto")
            .setPackage("com.example.grpc")
            .setOptions(DescriptorProtos.FileOptions.newBuilder()
                .setJavaPackage("com.example.grpc")
                .setJavaOuterClassname("TestServiceOuterClass")
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
                    .setOutputType(".com.example.grpc.OutputType")))
            .build();

        try {
            return Descriptors.FileDescriptor.buildFrom(proto, new Descriptors.FileDescriptor[] {});
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException("Failed to build test descriptor", e);
        }
    }

}
