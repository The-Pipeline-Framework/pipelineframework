package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Empty;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.grpc.MetricCollectingServerInterceptor;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pipelineframework.processor.ir.*;
import org.pipelineframework.telemetry.MetricRenamingConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        renderer.render(binding, Jsr269GenerationContext.create(processingEnv, tempDir, DeploymentRole.ORCHESTRATOR_CLIENT,
            java.util.Set.of(), null, descriptorSet));

        Path generatedSource = tempDir.resolve("com/example/orchestrator/OrchestratorApplication.java");
        String source = Files.readString(generatedSource);

        assertTrue(source.contains("Multi<InputType> inputMulti"));
        assertTrue(source.contains("InputTypeMapper inputTypeMapper"));
        assertTrue(source.contains("InputTypeDto.class"));
        assertTrue(source.contains(".map(inputTypeMapper::toGrpc)"));
        assertTrue(source.contains("deriveCliIdempotencyKey(actualInputList, actualInput)"));
        assertTrue(source.contains("executePipelineAsync(inputMulti, null, idempotencyKey, false)"));
        assertTrue(source.contains("disableObjectIngestAutostartForIngestOnce(args)"));
        assertTrue(source.contains("System.setProperty(\"pipeline.object-ingest.autostart\", \"false\")"));
    }

    @Test
    void rendersRestCliWithoutMapper() throws IOException {
        OrchestratorBinding binding = buildBinding("REST");
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));

        OrchestratorCliRenderer renderer = new OrchestratorCliRenderer();
        renderer.render(binding, Jsr269GenerationContext.create(processingEnv, tempDir, DeploymentRole.ORCHESTRATOR_CLIENT,
            java.util.Set.of(), null, null));

        Path generatedSource = tempDir.resolve("com/example/orchestrator/OrchestratorApplication.java");
        String source = Files.readString(generatedSource);

        assertTrue(source.contains("Multi<InputTypeDto> inputMulti"));
        assertTrue(source.contains("InputTypeDto.class"));
        assertFalse(source.contains("Mapper"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void generatedCliMetersCoexistWithGrpcInEitherRegistrationOrder(boolean grpcFirst) throws Exception {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));
        GenerationContext context = Jsr269GenerationContext.create(
            processingEnv, tempDir, DeploymentRole.ORCHESTRATOR_CLIENT, java.util.Set.of(), null, null);
        new OrchestratorCliRenderer().render(buildBinding("REST"), context);

        // Execute the emitted statements, so the registry test also covers the generator.
        String generated = Files.readString(tempDir.resolve("com/example/orchestrator/OrchestratorApplication.java"));
        List<String> metricStatements = generated.lines().map(String::strip)
            .filter(line -> line.startsWith("Tags grpcTags =")
                || line.startsWith("Timer.Sample sample =")
                || line.startsWith("Tags allTags =")
                || line.startsWith("registry.counter(")
                || line.startsWith("Counter.builder(")
                || line.startsWith("sample.stop("))
            .toList();
        assertEquals(5, metricStatements.size(), "The generated metering block must be exercised in full");
        Path fixture = tempDir.resolve("GeneratedCliMeters.java");
        Files.writeString(fixture, """
            import io.micrometer.core.instrument.*;
            public class GeneratedCliMeters {
                public void record(MeterRegistry registry, String grpcStatus) {
                    boolean hasRegistry = true;
                    %s
                }
            }
            """.formatted(String.join("\n", metricStatements)));
        var compiler = ToolProvider.getSystemJavaCompiler();
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        try (var files = compiler.getStandardFileManager(diagnostics, Locale.ROOT, StandardCharsets.UTF_8)) {
            var options = List.of("-proc:none", "-d", tempDir.toString(), "-classpath",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
            assertTrue(compiler.getTask(new StringWriter(), files, diagnostics, options, List.of(),
                files.getJavaFileObjects(fixture)).call(), diagnostics.getDiagnostics().toString());
        }

        var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        try (var loader = new URLClassLoader(new URL[] {tempDir.toUri().toURL()}, getClass().getClassLoader())) {
            registry.config().meterFilter(new MetricRenamingConfig().grpcRenameFilter());
            registry.config().onMeterRegistrationFailed((id, reason) -> {
                throw new IllegalArgumentException(id + ": " + reason);
            });
            Class<?> fixtureClass = loader.loadClass("GeneratedCliMeters");
            Object meters = fixtureClass.getConstructor().newInstance();
            var record = fixtureClass.getMethod("record", MeterRegistry.class, String.class);
            if (grpcFirst) {
                registerNativeGrpcMeters(registry);
            }
            for (String status : List.of("0", "2", "0", "2")) {
                record.invoke(meters, registry, status);
            }
            if (!grpcFirst) {
                registerNativeGrpcMeters(registry);
            }
            for (String metric : List.of("rpc.server.requests", "rpc.server.duration")) {
                long schemas = registry.find(metric).meters().stream()
                    .map(meter -> meter.getId().getTags().stream().map(tag -> tag.getKey()).collect(Collectors.toSet()))
                    .distinct().count();
                assertEquals(1, schemas, metric + " must have one tag schema");
            }
            assertEquals(4.0, registry.get("rpc.server.requests").tag("methodType", "CLI").counter().count());
            for (String status : List.of("0", "2")) {
                var timer = registry.get("rpc.server.duration").tag("methodType", "CLI")
                    .tag("rpc.grpc.status_code", status).timer();
                assertEquals(2, timer.count());
                assertTrue(timer.totalTime(TimeUnit.NANOSECONDS) >= 0);
            }
            String scrape = registry.scrape();
            assertTrue(scrape.contains("rpc_server_requests_messages_total"), scrape);
            assertTrue(scrape.contains("methodType=\"CLI\""), scrape);
            assertTrue(scrape.contains("rpc_grpc_status_code=\"0\""), scrape);
            assertTrue(scrape.contains("rpc_grpc_status_code=\"2\""), scrape);
        } finally {
            registry.close();
        }
    }

    private void registerNativeGrpcMeters(MeterRegistry registry) {
        var method = MethodDescriptor.<Empty, Empty>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName("NativeService/Run")
            .setRequestMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
            .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
            .build();
        var service = ServerServiceDefinition.builder("NativeService")
            .addMethod(method, (call, headers) -> new ServerCall.Listener<Empty>() {})
            .build();
        new MetricCollectingServerInterceptor(registry).preregisterService(service);
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
