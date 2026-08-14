package org.pipelineframework.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class TelemetryArchitectureTest {
    @Test
    void onlyProductionTelemetryRuntimeAccessesTheJvmGlobalSdk() throws Exception {
        Path sourceRoot = Path.of("src", "main", "java", "org", "pipelineframework");
        try (var paths = Files.walk(sourceRoot)) {
            List<Path> globalUsers = paths.filter(path -> path.toString().endsWith(".java"))
                .filter(path -> contains(path, "GlobalOpenTelemetry"))
                .toList();
            assertEquals(List.of(sourceRoot.resolve("telemetry/GlobalTelemetryRuntime.java")), globalUsers);
        }
    }

    @Test
    void pureObservationAndAttributeCoreDoesNotUseSdkOrCdi() throws Exception {
        for (String name : List.of("TelemetryObservation.java", "TelemetryAttributes.java", "TelemetryPolicy.java",
            "AwaitObservation.java", "AwaitTelemetryAttributes.java")) {
            String source = Files.readString(Path.of("src", "main", "java", "org", "pipelineframework", "telemetry", name));
            assertFalse(source.contains("io.opentelemetry"));
            assertFalse(source.contains("jakarta."));
        }
    }

    @Test
    void staticCompatibilityRuntimeDoesNotOwnInstrumentsOrTraceContext() throws Exception {
        String source = Files.readString(Path.of("src", "main", "java", "org", "pipelineframework",
            "telemetry", "TelemetryRuntimes.java"));
        assertFalse(source.contains("LongCounter"));
        assertFalse(source.contains("SpanContext"));
    }

    @Test
    void awaitTelemetryHasNoStaticSdkState() throws Exception {
        String source = Files.readString(Path.of("src", "main", "java", "org", "pipelineframework",
            "awaitable", "AwaitTelemetry.java"));
        assertFalse(source.contains("static volatile"));
        assertFalse(source.contains("GlobalOpenTelemetry"));
    }

    @Test
    void productionCodeDoesNotCacheSdkInstrumentsInStaticFields() throws Exception {
        Path sourceRoot = Path.of("src", "main", "java", "org", "pipelineframework");
        try (var paths = Files.walk(sourceRoot)) {
            List<Path> cached = paths.filter(path -> path.toString().endsWith(".java"))
                .filter(path -> contains(path, "static volatile LongCounter")
                    || contains(path, "static volatile DoubleHistogram")
                    || contains(path, "static volatile Meter"))
                .toList();
            assertEquals(List.of(), cached);
        }
    }

    @Test
    void productionExecutionDoesNotDependOnPipelineTelemetryFacade() throws Exception {
        Path sourceRoot = Path.of("src", "main", "java", "org", "pipelineframework");
        try (var paths = Files.walk(sourceRoot)) {
            List<Path> facadeUsers = paths.filter(path -> path.toString().endsWith(".java"))
                .filter(path -> !path.endsWith(Path.of("telemetry", "PipelineTelemetry.java")))
                .filter(path -> contains(path, "import org.pipelineframework.telemetry.PipelineTelemetry;"))
                .toList();
            assertEquals(List.of(), facadeUsers);
        }
    }

    @Test
    void internalEmittersDoNotCallGlobalRuntimeDirectly() throws Exception {
        Path sourceRoot = Path.of("src", "main", "java", "org", "pipelineframework");
        try (var paths = Files.walk(sourceRoot)) {
            List<Path> globalUsers = paths.filter(path -> path.toString().endsWith(".java"))
                .filter(path -> !path.endsWith(Path.of("telemetry", "TelemetryCompatibilityAccess.java")))
                .filter(path -> contains(path, "TelemetryRuntimes.global()"))
                .toList();
            assertEquals(List.of(), globalUsers);
        }
    }

    @Test
    void publicStaticTelemetryApisAreThinDelegates() throws Exception {
        Path sourceRoot = Path.of("src", "main", "java", "org", "pipelineframework");
        for (String relative : List.of(
            "telemetry/HttpMetrics.java",
            "telemetry/RpcMetrics.java",
            "telemetry/ApmCompatibilityMetrics.java",
            "telemetry/BackpressureBufferMetrics.java",
            "telemetry/GrpcClientTracing.java",
            "command/CommandEffectMetrics.java",
            "orchestrator/DeadLetterMetrics.java",
            "reject/ItemRejectMetrics.java")) {
            String source = Files.readString(sourceRoot.resolve(relative));
            assertFalse(source.contains(".meter("), relative);
            assertFalse(source.contains(".tracer("), relative);
            assertFalse(source.contains("Span.current()"), relative);
            assertFalse(source.contains("Attributes.builder()"), relative);
        }
    }

    @Test
    void retryCompatibilityApiDoesNotOwnTraceRoutingState() throws Exception {
        String source = Files.readString(Path.of("src", "main", "java", "org", "pipelineframework",
            "telemetry", "PipelineRetryTelemetry.java"));
        assertFalse(source.contains("Span.current()"));
        assertFalse(source.contains("ConcurrentMap"));
        assertFalse(source.contains("AtomicReference"));
    }

    @Test
    void executionRunContextIsNotOwnedByCompatibilityFacade() throws Exception {
        String facade = Files.readString(Path.of("src", "main", "java", "org", "pipelineframework",
            "telemetry", "PipelineTelemetry.java"));
        assertFalse(facade.contains("record RunContext"));
        for (String executionType : List.of("PipelineRunner.java", "PipelineStepExecutor.java", "ExecutionHooks.java")) {
            String source = Files.readString(Path.of("src", "main", "java", "org", "pipelineframework", executionType));
            assertFalse(source.contains("PipelineTelemetry"));
        }
    }

    private static boolean contains(Path path, String value) {
        try {
            return Files.readString(path).contains(value);
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }
}
