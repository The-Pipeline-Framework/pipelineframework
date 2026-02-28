package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.nio.file.Files;
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

class GrpcServiceAdapterRendererTest {

    private GrpcServiceAdapterRenderer renderer;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        renderer = new GrpcServiceAdapterRenderer(org.pipelineframework.processor.ir.GenerationTarget.GRPC_SERVICE);
    }

    @Test
    void testRenderUnaryUnaryGrpcServiceAdapter() {
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
        var context = new GenerationContext(processingEnv, tempDir, DeploymentRole.PIPELINE_SERVER,
            java.util.Set.of(), null, null);

        assertDoesNotThrow(() -> renderer.render(binding, context));
        String source = readGeneratedService("TestService");
        assertTrue(source.contains("onTermination().invoke"),
            "Expected onTermination handler for gRPC server metrics");
        assertTrue(source.contains("Status.CANCELLED"),
            "Expected cancellation handling for gRPC server metrics");
    }
    
    @Test
    void testRenderUnaryStreamingGrpcServiceAdapter() {
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
        var context = new GenerationContext(processingEnv, tempDir, DeploymentRole.PIPELINE_SERVER,
            java.util.Set.of(), null, null);

        assertDoesNotThrow(() -> renderer.render(binding, context));
        String source = readGeneratedService("TestService");
        assertTrue(source.contains("onTermination().invoke"),
            "Expected onTermination handler for gRPC server metrics");
        assertTrue(source.contains("Status.CANCELLED"),
            "Expected cancellation handling for gRPC server metrics");
    }

    @Test
    void testRenderStreamingUnaryGrpcServiceAdapter() {
        PipelineStepModel model = createModel(StreamingShape.STREAMING_UNARY);

        Descriptors.FileDescriptor fileDescriptor = buildFileDescriptor();
        Descriptors.ServiceDescriptor serviceDescriptor = fileDescriptor.findServiceByName("TestService");
        Descriptors.MethodDescriptor methodDescriptor = serviceDescriptor.findMethodByName("remoteProcess");
        GrpcBinding binding = new GrpcBinding(model, serviceDescriptor, methodDescriptor);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getElementUtils()).thenReturn(null);
        when(processingEnv.getTypeUtils()).thenReturn(null);
        when(processingEnv.getFiler()).thenReturn(null);
        when(processingEnv.getMessager()).thenReturn(null);

        var context = new GenerationContext(processingEnv, tempDir, DeploymentRole.PIPELINE_SERVER,
            java.util.Set.of(), null, null);

        assertDoesNotThrow(() -> renderer.render(binding, context));
        String source = readGeneratedService("TestService");
        assertTrue(source.contains("onTermination().invoke"),
            "Expected onTermination handler for gRPC server metrics");
        assertTrue(source.contains("Status.CANCELLED"),
            "Expected cancellation handling for gRPC server metrics");
    }

    @Test
    void testRenderStreamingStreamingGrpcServiceAdapter() {
        PipelineStepModel model = createModel(StreamingShape.STREAMING_STREAMING);

        Descriptors.FileDescriptor fileDescriptor = buildFileDescriptor();
        Descriptors.ServiceDescriptor serviceDescriptor = fileDescriptor.findServiceByName("TestService");
        Descriptors.MethodDescriptor methodDescriptor = serviceDescriptor.findMethodByName("remoteProcess");
        GrpcBinding binding = new GrpcBinding(model, serviceDescriptor, methodDescriptor);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getElementUtils()).thenReturn(null);
        when(processingEnv.getTypeUtils()).thenReturn(null);
        when(processingEnv.getFiler()).thenReturn(null);
        when(processingEnv.getMessager()).thenReturn(null);

        var context = new GenerationContext(processingEnv, tempDir, DeploymentRole.PIPELINE_SERVER,
            java.util.Set.of(), null, null);

        assertDoesNotThrow(() -> renderer.render(binding, context));
        String source = readGeneratedService("TestService");
        assertTrue(source.contains("onTermination().invoke"),
            "Expected onTermination handler for gRPC server metrics");
        assertTrue(source.contains("Status.CANCELLED"),
            "Expected cancellation handling for gRPC server metrics");
    }

    @Test
    void testRenderSideEffectServiceUsesParameterizedPlugin() throws IOException {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ObserveOutputTypeSideEffectService")
            .generatedName("PersistenceOutputTypeSideEffect")
            .servicePackage("com.example")
            .serviceClassName(ClassName.get("org.pipelineframework.plugin.persistence", "PersistenceService"))
            .inputMapping(new TypeMapping(ClassName.get("com.example.domain", "OutputType"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.domain", "OutputType"), null, false))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .enabledTargets(java.util.Set.of(GenerationTarget.GRPC_SERVICE))
            .deploymentRole(DeploymentRole.PLUGIN_SERVER)
            .sideEffect(true)
            .build();

        Descriptors.FileDescriptor fileDescriptor = buildSideEffectDescriptor();
        Descriptors.ServiceDescriptor serviceDescriptor = fileDescriptor.findServiceByName("ObserveOutputTypeSideEffectService");
        Descriptors.MethodDescriptor methodDescriptor = serviceDescriptor.findMethodByName("remoteProcess");
        GrpcBinding binding = new GrpcBinding(model, serviceDescriptor, methodDescriptor);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getElementUtils()).thenReturn(null);
        when(processingEnv.getTypeUtils()).thenReturn(null);
        when(processingEnv.getFiler()).thenReturn(null);
        when(processingEnv.getMessager()).thenReturn(null);

        var context = new GenerationContext(processingEnv, tempDir, DeploymentRole.PLUGIN_SERVER,
            java.util.Set.of(), null, null);
        renderer.render(binding, context);

        String source = readGeneratedService("PersistenceOutputTypeSideEffect");
        assertTrue(source.contains("ObserveOutputTypeSideEffectService service"));
        assertTrue(source.contains("protected ObserveOutputTypeSideEffectService getService()"));
        assertTrue(source.contains("return service"));
        assertTrue(source.contains("onTermination().invoke"),
            "Expected onTermination handler for gRPC server metrics");
        assertTrue(source.contains("Status.CANCELLED"),
            "Expected cancellation handling for gRPC server metrics");
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
            .enabledTargets(java.util.Set.of(GenerationTarget.GRPC_SERVICE))
            .build();
    }

    private String readGeneratedService(String baseName) {
        Path generated = tempDir.resolve("com/example/pipeline/" + baseName + "GrpcService.java");
        try {
            return Files.readString(generated);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read generated gRPC service: " + generated, e);
        }
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

    private Descriptors.FileDescriptor buildSideEffectDescriptor() {
        DescriptorProtos.FileDescriptorProto proto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("observe_output_type.proto")
            .setPackage("com.example.grpc")
            .setOptions(DescriptorProtos.FileOptions.newBuilder()
                .setJavaPackage("com.example.grpc")
                .setJavaOuterClassname("ObserveOutputTypeOuterClass")
                .build())
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                .setName("OutputType"))
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("ObserveOutputTypeSideEffectService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("remoteProcess")
                    .setInputType(".com.example.grpc.OutputType")
                    .setOutputType(".com.example.grpc.OutputType")))
            .build();

        try {
            return Descriptors.FileDescriptor.buildFrom(proto, new Descriptors.FileDescriptor[] {});
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException("Failed to build test descriptor", e);
        }
    }

    @Test
    void testRenderGrpcServiceWithVirtualThreads() throws IOException {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("TestService")
            .servicePackage("com.example")
            .serviceClassName(ClassName.get("com.example", "TestService"))
            .inputMapping(createTypeMapping("InputType"))
            .outputMapping(createTypeMapping("OutputType"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.VIRTUAL_THREADS)
            .enabledTargets(java.util.Set.of(GenerationTarget.GRPC_SERVICE))
            .build();

        Descriptors.FileDescriptor fileDescriptor = buildFileDescriptor();
        Descriptors.ServiceDescriptor serviceDescriptor = fileDescriptor.findServiceByName("TestService");
        Descriptors.MethodDescriptor methodDescriptor = serviceDescriptor.findMethodByName("remoteProcess");
        GrpcBinding binding = new GrpcBinding(model, serviceDescriptor, methodDescriptor);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getElementUtils()).thenReturn(null);
        when(processingEnv.getTypeUtils()).thenReturn(null);
        when(processingEnv.getFiler()).thenReturn(null);
        when(processingEnv.getMessager()).thenReturn(null);

        var context = new GenerationContext(processingEnv, tempDir, DeploymentRole.PIPELINE_SERVER,
            java.util.Set.of(), null, null);

        renderer.render(binding, context);
        String source = readGeneratedService("TestService");

        assertTrue(source.contains("@RunOnVirtualThread"));
    }

    @Test
    void testRenderCacheSideEffectWithoutMappers() throws IOException {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("CacheSideEffectService")
            .generatedName("CacheSideEffect")
            .servicePackage("com.example")
            .serviceClassName(ClassName.get("org.pipelineframework.plugin.cache", "CacheService"))
            .inputMapping(createTypeMapping("CacheKey"))
            .outputMapping(createTypeMapping("CacheValue"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .enabledTargets(java.util.Set.of(GenerationTarget.GRPC_SERVICE))
            .sideEffect(true)
            .build();

        DescriptorProtos.FileDescriptorProto proto = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("cache.proto")
            .setPackage("com.example.grpc")
            .setOptions(DescriptorProtos.FileOptions.newBuilder()
                .setJavaPackage("com.example.grpc")
                .setJavaOuterClassname("CacheOuterClass")
                .build())
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("CacheKey"))
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("CacheValue"))
            .addService(DescriptorProtos.ServiceDescriptorProto.newBuilder()
                .setName("CacheSideEffectService")
                .addMethod(DescriptorProtos.MethodDescriptorProto.newBuilder()
                    .setName("remoteProcess")
                    .setInputType(".com.example.grpc.CacheKey")
                    .setOutputType(".com.example.grpc.CacheValue")))
            .build();

        Descriptors.FileDescriptor fileDescriptor;
        try {
            fileDescriptor = Descriptors.FileDescriptor.buildFrom(proto, new Descriptors.FileDescriptor[] {});
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException("Failed to build test descriptor", e);
        }

        Descriptors.ServiceDescriptor serviceDescriptor = fileDescriptor.findServiceByName("CacheSideEffectService");
        Descriptors.MethodDescriptor methodDescriptor = serviceDescriptor.findMethodByName("remoteProcess");
        GrpcBinding binding = new GrpcBinding(model, serviceDescriptor, methodDescriptor);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getElementUtils()).thenReturn(null);
        when(processingEnv.getTypeUtils()).thenReturn(null);
        when(processingEnv.getFiler()).thenReturn(null);
        when(processingEnv.getMessager()).thenReturn(null);

        var context = new GenerationContext(processingEnv, tempDir, DeploymentRole.PIPELINE_SERVER,
            java.util.Set.of(), null, null);

        renderer.render(binding, context);

        Path generated = tempDir.resolve("com/example/pipeline/CacheSideEffectGrpcService.java");
        String source = Files.readString(generated);

        assertFalse(source.contains("inboundMapper"));
        assertFalse(source.contains("outboundMapper"));
    }

    @Test
    void testRenderGrpcServiceWithAnnotations() throws IOException {
        PipelineStepModel model = createModel(StreamingShape.UNARY_UNARY);

        Descriptors.FileDescriptor fileDescriptor = buildFileDescriptor();
        Descriptors.ServiceDescriptor serviceDescriptor = fileDescriptor.findServiceByName("TestService");
        Descriptors.MethodDescriptor methodDescriptor = serviceDescriptor.findMethodByName("remoteProcess");
        GrpcBinding binding = new GrpcBinding(model, serviceDescriptor, methodDescriptor);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getElementUtils()).thenReturn(null);
        when(processingEnv.getTypeUtils()).thenReturn(null);
        when(processingEnv.getFiler()).thenReturn(null);
        when(processingEnv.getMessager()).thenReturn(null);

        var context = new GenerationContext(processingEnv, tempDir, DeploymentRole.PIPELINE_SERVER,
            java.util.Set.of(), null, null);

        renderer.render(binding, context);
        String source = readGeneratedService("TestService");

        assertTrue(source.contains("@GrpcService"));
        assertTrue(source.contains("@Singleton"));
        assertTrue(source.contains("@Unremovable"));
        assertTrue(source.contains("@GeneratedRole"));
    }

    @Test
    void verifyTargetReturnsGrpcService() {
        assertEquals(GenerationTarget.GRPC_SERVICE, renderer.target());
    }

    @Test
    void testRenderIncludesRpcMetrics() throws IOException {
        PipelineStepModel model = createModel(StreamingShape.UNARY_UNARY);

        Descriptors.FileDescriptor fileDescriptor = buildFileDescriptor();
        Descriptors.ServiceDescriptor serviceDescriptor = fileDescriptor.findServiceByName("TestService");
        Descriptors.MethodDescriptor methodDescriptor = serviceDescriptor.findMethodByName("remoteProcess");
        GrpcBinding binding = new GrpcBinding(model, serviceDescriptor, methodDescriptor);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getElementUtils()).thenReturn(null);
        when(processingEnv.getTypeUtils()).thenReturn(null);
        when(processingEnv.getFiler()).thenReturn(null);
        when(processingEnv.getMessager()).thenReturn(null);

        var context = new GenerationContext(processingEnv, tempDir, DeploymentRole.PIPELINE_SERVER,
            java.util.Set.of(), null, null);

        renderer.render(binding, context);
        String source = readGeneratedService("TestService");

        assertTrue(source.contains("RpcMetrics.recordGrpcServer"));
        assertTrue(source.contains("long startTime = System.nanoTime()"));
    }

}