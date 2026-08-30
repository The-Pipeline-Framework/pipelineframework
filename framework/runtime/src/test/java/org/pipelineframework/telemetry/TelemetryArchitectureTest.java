package org.pipelineframework.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
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
        Path telemetry = Path.of("src", "main", "java", "org", "pipelineframework", "telemetry");
        try (Stream<Path> paths = Stream.concat(Files.walk(telemetry.resolve("observation")),
            Files.walk(telemetry.resolve("derivation")))) {
            for (Path path : paths.filter(value -> value.toString().endsWith(".java")).toList()) {
                String source = Files.readString(path);
                assertFalse(source.contains("io.opentelemetry"), path.toString());
                assertFalse(source.contains("jakarta."), path.toString());
                assertFalse(source.contains("org.pipelineframework.awaitable"), path.toString());
                assertFalse(source.contains("TelemetryRuntime"), path.toString());
            }
        }
        for (String name : List.of("TelemetryPolicy.java", "AwaitObservation.java", "AwaitTelemetryAttributes.java",
            "PipelineMetricAttributes.java", "PipelineSpanAttributes.java")) {
            String source = Files.readString(telemetry.resolve(name));
            assertFalse(source.contains("io.opentelemetry"), name);
            assertFalse(source.contains("jakarta."), name);
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

    @Test
    void terminalStepMeaningIsClassifiedOnlyByPureDerivation() throws Exception {
        Path sourceRoot = Path.of("src", "main", "java", "org", "pipelineframework");
        try (var paths = Files.walk(sourceRoot)) {
            List<Path> classifiers = paths.filter(path -> path.toString().endsWith(".java"))
                .filter(path -> contains(path, "instanceof StepObservation.Completed")
                    || contains(path, "instanceof StepObservation.Failed")
                    || contains(path, "instanceof StepObservation.Cancelled"))
                .toList();
            assertEquals(List.of(sourceRoot.resolve(
                "telemetry/derivation/StepTelemetryDerivation.java")), classifiers);
        }
        String instrumentation = Files.readString(sourceRoot.resolve(
            "telemetry/PipelineStepInstrumentation.java"));
        assertFalse(instrumentation.contains("metrics.stepFinished("));
        assertFalse(instrumentation.contains("replay.complete(replayScope, failure"));
        assertFalse(instrumentation.contains("tracing.finish(span, cancelled"));
    }

    @Test
    void focusedAdaptersAcceptDerivedSignalsInsteadOfTerminalParameterBags() {
        Set<Class<?>> signalTypes = Set.of(
            org.pipelineframework.telemetry.derivation.StepTelemetryDerivation.MetricStarted.class,
            org.pipelineframework.telemetry.derivation.StepTelemetryDerivation.MetricFinished.class,
            org.pipelineframework.telemetry.derivation.RunTelemetryDerivation.MetricStarted.class,
            org.pipelineframework.telemetry.derivation.RunTelemetryDerivation.MetricFinished.class,
            org.pipelineframework.telemetry.derivation.PipelineSloDerivation.ThroughputSignal.class,
            org.pipelineframework.telemetry.derivation.PipelineSloDerivation.SuccessSignal.class,
            org.pipelineframework.telemetry.derivation.RetryTelemetryDerivation.MetricSignal.class);
        for (var method : PipelineMetricsRecorder.class.getDeclaredMethods()) {
            if (method.getName().equals("record") && method.getParameterCount() > 0) {
                assertTrue(signalTypes.contains(method.getParameterTypes()[0]), method.toString());
            }
        }
    }

    @Test
    void metricsAdaptersDoNotManipulateSpansAndPureCoreDoesNotManipulateInstruments() throws Exception {
        Path pure = Path.of("src", "main", "java", "org", "pipelineframework", "telemetry");
        for (String name : List.of(
            "PipelineMetricsRecorder.java",
            "QueryObservationMetrics.java",
            "orchestrator/TransitionWorkerMetrics.java")) {
            Path path = name.startsWith("orchestrator/")
                ? Path.of("src", "main", "java", "org", "pipelineframework", name)
                : Path.of("src", "main", "java", "org", "pipelineframework", "telemetry", name);
            String source = Files.readString(path);
            assertFalse(source.contains("Span.current()"), name);
            assertFalse(source.contains("SpanBuilder"), name);
        }
        String queryTracing = Files.readString(pure.resolve("QueryObservationTracing.java"));
        assertFalse(queryTracing.contains("LongHistogram"));
        assertFalse(queryTracing.contains(".meter("));
        for (String directory : List.of("observation", "derivation")) {
            try (var paths = Files.walk(pure.resolve(directory))) {
                for (Path path : paths.filter(value -> value.toString().endsWith(".java")).toList()) {
                    String source = Files.readString(path);
                    assertFalse(source.contains("LongCounter"), path.toString());
                    assertFalse(source.contains("DoubleHistogram"), path.toString());
                    assertFalse(source.contains(".meter("), path.toString());
                }
            }
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
