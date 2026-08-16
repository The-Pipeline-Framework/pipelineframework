package org.pipelineframework.processor;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Provider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.pipelineframework.PipelineRunner;
import org.pipelineframework.PipelineRunnerTestHarness;
import org.pipelineframework.processor.composition.CompiledPipelineLocation;
import org.pipelineframework.processor.composition.DefinitionLocalLocation;
import org.pipelineframework.processor.composition.LocalPipelineInvocationClassName;
import org.pipelineframework.processor.composition.PipelineReference;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;
import org.pipelineframework.invocation.PipelineRecursionLimitExceededException;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Isolated
class PipelineRecursionProductizationTest {

    private static final PipelineReference ROOT = new PipelineReference("$root");
    private static final PipelineReference AGENT = new PipelineReference("agent");
    private static final String ROOT_INVOCATION = invocationName(List.of(), ROOT, "Call agent");
    private static final String RECURSIVE_INVOCATION = invocationName(
        List.of(new DefinitionLocalLocation(ROOT, "Call agent")), AGENT, "Recur");

    @TempDir
    Path tempDir;

    @Test
    void generatedDirectSelfRecursionRoutesBoundsUnwindsAndKeepsRootOwnership() throws Exception {
        Fixture fixture = compile("recursive", recursionYaml(), recursionSources());
        Fixture repeated = compile("recursive-repeat", recursionYaml(), recursionSources());

        assertThat(fixture.compilation()).succeeded();
        assertThat(repeated.compilation()).succeeded();
        String contract = fixture.metadata("pipeline-contract.json");
        assertEquals(contract, repeated.metadata("pipeline-contract.json"));
        assertTrue(contract.contains("\"targetDefinitionId\": \"agent\""));
        assertTrue(contract.contains("\"RETURN\""));
        assertTrue(contract.length() < 30_000, "Recursive composition contract must remain finite");
        String branching = fixture.metadata("branching.json");
        assertTrue(branching.contains("\"runtimeStepClass\": "
            + "\"com.example.recursion.pipeline." + ROOT_INVOCATION + "\",\n"
            + "      \"inputRuntimeClass\": \"com.example.recursion.domain.State\""),
            "Invocation input metadata must use the generated method signature, not its first accepted variant");

        String recursiveSource = Files.readString(
            fixture.generatedClass(RECURSIVE_INVOCATION));
        assertTrue(recursiveSource.contains("Provider<" + RECURSIVE_INVOCATION + ">"));
        assertTrue(recursiveSource.contains("recursiveOneToOne(runner, \"agent\", \"Recur\""));
        assertTrue(recursiveSource.contains("effectiveConfig()).applyOneToOne(input)"));
        assertFalse(recursiveSource.contains("ExecutionRecord"));
        assertFalse(recursiveSource.contains("PipelineTelemetry.RunContext"));

        Path classes = compileGeneratedFixture(fixture, recursionSources(), List.of(
            ROOT_INVOCATION,
            RECURSIVE_INVOCATION,
            "ProcessDecideLocalClientStep",
            "ProcessCompleteLocalClientStep",
            "ProcessUnwindLocalClientStep"));
        Files.createDirectories(classes.resolve("META-INF/pipeline"));
        Files.writeString(classes.resolve("META-INF/pipeline/branching.json"), branching);

        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(new java.net.URL[]{classes.toUri().toURL()}, previous)) {
            Thread.currentThread().setContextClassLoader(loader);
            verifyBaseCaseAndRootOwnership(loader);
            verifyMultipleFramesAndExactLimit(loader);
            verifyLimitFailure(loader);
            verifyStackSafety(loader);
            verifyFailurePropagation(loader);
            verifyCancellation(loader);
        } finally {
            PipelineContextHolder.clear();
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    private void verifyBaseCaseAndRootOwnership(ClassLoader loader) throws Exception {
        PipelineRunnerTestHarness.Harness harness = PipelineRunnerTestHarness.createHarness().maxRecursiveDepth(8);
        Object root = invocationGraph(loader, harness.runner());
        PipelineContext context = new PipelineContext("release-recursive", "live", "default");
        AtomicInteger subscriptions = new AtomicInteger();
        Object baseState = state(loader, 0);
        PipelineContextHolder.set(context);
        PipelineRunner.ExecutionResult execution = harness.runner().runWithContext(
            Uni.createFrom().deferred(() -> {
                subscriptions.incrementAndGet();
                return Uni.createFrom().item(baseState);
            }), List.of(root));
        Object result = ((Uni<?>) execution.result()).await().indefinitely();

        assertEquals("Result[value=done]", result.toString());
        assertEquals(1, staticInt(loader, "com.example.recursion.UnwindService", "calls"));
        assertSame(context, staticField(loader, "com.example.recursion.UnwindService", "context"));
        assertEquals(1, subscriptions.get());
        assertTrue(execution.terminalOutputPublished());
        verify(harness.runTelemetry(), times(1)).startRun(any(), anyInt(), any(), anyInt());
        verify(harness.publisher(), times(1)).publish(any());
        harness.verifyRootOrderAppliedOnce();
    }

    private void verifyMultipleFramesAndExactLimit(ClassLoader loader) throws Exception {
        resetCounters(loader);
        PipelineRunnerTestHarness.Harness harness = PipelineRunnerTestHarness.createHarness().maxRecursiveDepth(3);
        Object root = invocationGraph(loader, harness.runner());
        Object result = run(loader, harness.runner(), root, 3);

        assertEquals("Result[value=done]", result.toString());
        assertEquals(4, staticInt(loader, "com.example.recursion.UnwindService", "calls"),
            "Each recursive invocation must return through its caller suffix exactly once");

        Object repeated = run(loader, harness.runner(), root, 1);
        assertEquals("Result[value=done]", repeated.toString());
        assertEquals(6, staticInt(loader, "com.example.recursion.UnwindService", "calls"),
            "A generated recursive bean must not retain the prior root invocation path");
    }

    private void verifyLimitFailure(ClassLoader loader) throws Exception {
        PipelineRunnerTestHarness.Harness harness = PipelineRunnerTestHarness.createHarness().maxRecursiveDepth(2);
        RuntimeException failure = assertThrows(RuntimeException.class,
            () -> run(loader, harness.runner(), invocationGraph(loader, harness.runner()), 3));
        Throwable cause = rootCause(failure);

        assertTrue(cause instanceof PipelineRecursionLimitExceededException);
        PipelineRecursionLimitExceededException limit = (PipelineRecursionLimitExceededException) cause;
        assertEquals(3, limit.attemptedDepth());
        assertEquals(2, limit.maximumDepth());
        assertEquals("agent", limit.definitionId());
        assertEquals("Recur", limit.callsiteId());
    }

    private void verifyStackSafety(ClassLoader loader) throws Exception {
        resetCounters(loader);
        int supportedDepth = 512;
        PipelineRunnerTestHarness.Harness harness = PipelineRunnerTestHarness.createHarness()
            .maxRecursiveDepth(supportedDepth);
        Object result = run(loader, harness.runner(), invocationGraph(loader, harness.runner()), supportedDepth);

        assertEquals("Result[value=done]", result.toString());
        assertEquals(supportedDepth + 1, staticInt(loader, "com.example.recursion.UnwindService", "calls"));
    }

    private void verifyFailurePropagation(ClassLoader loader) throws Exception {
        resetCounters(loader);
        PipelineRunnerTestHarness.Harness harness = PipelineRunnerTestHarness.createHarness().maxRecursiveDepth(8);
        Field failAt = loader.loadClass("com.example.recursion.DecideService").getField("failAt");
        failAt.setInt(null, 1);
        try {
            RuntimeException failure = assertThrows(RuntimeException.class,
                () -> run(loader, harness.runner(), invocationGraph(loader, harness.runner()), 3));

            assertEquals("recursive-boom", rootCause(failure).getMessage());
            assertEquals(1, staticInt(loader, "com.example.recursion.DecideService", "failures"));
        } finally {
            failAt.setInt(null, Integer.MIN_VALUE);
        }
    }

    private void verifyCancellation(ClassLoader loader) throws Exception {
        PipelineRunnerTestHarness.Harness harness = PipelineRunnerTestHarness.createHarness().maxRecursiveDepth(8);
        Object root = invocationGraph(loader, harness.runner());
        PipelineRunner.ExecutionResult execution = harness.runner().runWithContext(
            Uni.createFrom().item(state(loader, -2)), List.of(root));
        var cancellable = ((Uni<?>) execution.result()).subscribe().with(ignored -> { }, ignored -> { });
        cancellable.cancel();

        CountDownLatch cancellation = (CountDownLatch) staticField(
            loader, "com.example.recursion.DecideService", "cancellation");
        assertTrue(cancellation.await(1, TimeUnit.SECONDS));
    }

    private Object run(ClassLoader loader, PipelineRunner runner, Object root, int remaining) throws Exception {
        PipelineRunner.ExecutionResult execution = runner.runWithContext(
            Uni.createFrom().item(state(loader, remaining)), List.of(root));
        return ((Uni<?>) execution.result()).await().indefinitely();
    }

    private Object invocationGraph(ClassLoader loader, PipelineRunner runner) throws Exception {
        Object recursive = loader.loadClass(
            "com.example.recursion.pipeline." + RECURSIVE_INVOCATION)
            .getConstructor().newInstance();
        set(recursive, "runner", runner);
        set(recursive, "child0", client(loader, "ProcessDecideLocalClientStep", "DecideService"));
        Provider<Object> self = () -> recursive;
        set(recursive, "child1", self);
        set(recursive, "child2", client(loader, "ProcessCompleteLocalClientStep", "CompleteService"));
        set(recursive, "child3", client(loader, "ProcessUnwindLocalClientStep", "UnwindService"));

        Object root = loader.loadClass("com.example.recursion.pipeline." + ROOT_INVOCATION)
            .getConstructor().newInstance();
        set(root, "runner", runner);
        set(root, "child0", client(loader, "ProcessDecideLocalClientStep", "DecideService"));
        set(root, "child1", recursive);
        set(root, "child2", client(loader, "ProcessCompleteLocalClientStep", "CompleteService"));
        set(root, "child3", client(loader, "ProcessUnwindLocalClientStep", "UnwindService"));
        return root;
    }

    private static String invocationName(
        List<DefinitionLocalLocation> path,
        PipelineReference definition,
        String stepId
    ) {
        return LocalPipelineInvocationClassName.simpleName(new CompiledPipelineLocation(
            path,
            new DefinitionLocalLocation(definition, stepId)));
    }

    private Object client(ClassLoader loader, String clientName, String serviceName) throws Exception {
        Object client = loader.loadClass("com.example.recursion.pipeline." + clientName).getConstructor().newInstance();
        Object service = loader.loadClass("com.example.recursion." + serviceName).getConstructor().newInstance();
        set(client, "service", service);
        return client;
    }

    private Object state(ClassLoader loader, int remaining) throws Exception {
        return loader.loadClass("com.example.recursion.domain.Continue").getConstructor(int.class).newInstance(remaining);
    }

    private void resetCounters(ClassLoader loader) throws Exception {
        loader.loadClass("com.example.recursion.UnwindService").getField("calls").setInt(null, 0);
        loader.loadClass("com.example.recursion.DecideService").getField("failures").setInt(null, 0);
    }

    private static int staticInt(ClassLoader loader, String className, String field) throws Exception {
        return loader.loadClass(className).getField(field).getInt(null);
    }

    private static Object staticField(ClassLoader loader, String className, String field) throws Exception {
        return loader.loadClass(className).getField(field).get(null);
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static void set(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private Fixture compile(String id, String yaml, Map<String, String> sources) throws IOException {
        Path projectRoot = tempDir.resolve(id);
        Files.createDirectories(projectRoot);
        Files.writeString(projectRoot.resolve("pom.xml"),
            "<project><modelVersion>4.0.0</modelVersion><groupId>x</groupId><artifactId>x</artifactId><version>1</version></project>");
        Path generated = projectRoot.resolve("module/target/generated-sources/pipeline");
        Files.createDirectories(generated);
        Path config = projectRoot.resolve("pipeline.yaml");
        Files.writeString(config, yaml);
        List<JavaFileObject> files = sources.entrySet().stream()
            .map(entry -> JavaFileObjects.forSourceString(entry.getKey(), entry.getValue()))
            .toList();
        Compilation compilation = Compiler.javac().withProcessors(new PipelineStepProcessor()).withOptions(
            "-Apipeline.config=" + config.toString().replace('\\', '/'),
            "-Apipeline.generatedSourcesDir=" + generated.toString().replace('\\', '/'),
            "-Apipeline.transport=LOCAL").compile(files);
        return new Fixture(projectRoot, generated, compilation);
    }

    private Path compileGeneratedFixture(Fixture fixture, Map<String, String> sources, List<String> generatedNames)
            throws IOException {
        Path sourceRoot = fixture.projectRoot().resolve("runtime-src");
        Path classes = fixture.projectRoot().resolve("runtime-classes");
        Files.createDirectories(sourceRoot);
        Files.createDirectories(classes);
        List<Path> javaSources = new ArrayList<>();
        for (Map.Entry<String, String> source : sources.entrySet()) {
            Path file = sourceRoot.resolve(source.getKey().replace('.', '/') + ".java");
            Files.createDirectories(file.getParent());
            Files.writeString(file, source.getValue());
            javaSources.add(file);
        }
        for (String generatedName : generatedNames) {
            javaSources.add(fixture.generatedClass(generatedName));
        }
        var compiler = ToolProvider.getSystemJavaCompiler();
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        try (var fileManager = compiler.getStandardFileManager(null, null, null)) {
            var units = fileManager.getJavaFileObjectsFromPaths(javaSources);
            boolean success = compiler.getTask(null, fileManager, diagnostics,
                List.of("-proc:none", "-classpath", System.getProperty("java.class.path"), "-d", classes.toString()),
                null, units).call();
            assertTrue(success, () -> diagnostics.getDiagnostics().stream()
                .map(diagnostic -> diagnostic.getKind() + ": " + diagnostic.getMessage(null))
                .collect(java.util.stream.Collectors.joining(System.lineSeparator())));
        }
        return classes;
    }

    private String recursionYaml() {
        return """
            version: 3
            appName: Structured recursion
            basePackage: com.example.recursion
            transport: LOCAL
            contract: { input: State, output: Result }
            types:
              Continue: { fields: [[remaining, int32]] }
              Complete: { fields: [[remaining, int32]] }
              State: { variants: { continue: Continue, complete: Complete } }
              Result: { fields: [[value, string]] }
            pipelines:
              agent:
                input: State
                output: Result
                steps:
                  - { name: Decide, service: com.example.recursion.DecideService, cardinality: ONE_TO_ONE, input: State, output: State, java: { input: com.example.recursion.domain.State, output: com.example.recursion.domain.State } }
                  - { name: Recur, pipeline: agent, cardinality: ONE_TO_ONE, input: State, output: Result, accepts: [Continue], java: { input: com.example.recursion.domain.Continue, output: com.example.recursion.domain.Result } }
                  - { name: Complete, service: com.example.recursion.CompleteService, cardinality: ONE_TO_ONE, input: State, output: Result, accepts: [Complete], java: { input: com.example.recursion.domain.Complete, output: com.example.recursion.domain.Result } }
                  - { name: Unwind, service: com.example.recursion.UnwindService, cardinality: ONE_TO_ONE, input: Result, output: Result, terminal: true, java: { input: com.example.recursion.domain.Result, output: com.example.recursion.domain.Result } }
            steps:
              - { name: Call agent, pipeline: agent, cardinality: ONE_TO_ONE, input: State, output: Result, terminal: true, java: { input: com.example.recursion.domain.State, output: com.example.recursion.domain.Result } }
            """;
    }

    private Map<String, String> recursionSources() {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("com.example.recursion.domain.State", "package com.example.recursion.domain; public sealed interface State permits Continue, Complete { int remaining(); }");
        sources.put("com.example.recursion.domain.Continue", "package com.example.recursion.domain; public record Continue(int remaining) implements State { }");
        sources.put("com.example.recursion.domain.Complete", "package com.example.recursion.domain; public record Complete(int remaining) implements State { }");
        sources.put("com.example.recursion.domain.Result", "package com.example.recursion.domain; public record Result(String value) { }");
        sources.put("com.example.recursion.DecideService", """
            package com.example.recursion;
            import com.example.recursion.domain.*;
            public class DecideService implements org.pipelineframework.service.ReactiveService<State, State> {
              public static int failures;
              public static int failAt = Integer.MIN_VALUE;
              public static java.util.concurrent.CountDownLatch cancellation = new java.util.concurrent.CountDownLatch(1);
              public io.smallrye.mutiny.Uni<State> process(State input) {
                if (input.remaining() == failAt) { failures++; return io.smallrye.mutiny.Uni.createFrom().failure(new org.pipelineframework.step.NonRetryableException("recursive-boom")); }
                if (input.remaining() == -2) { return io.smallrye.mutiny.Uni.createFrom().emitter(emitter -> emitter.onTermination(cancellation::countDown)); }
                return io.smallrye.mutiny.Uni.createFrom().item(input.remaining() > 0
                    ? new Continue(input.remaining() - 1) : new Complete(0));
              }
            }
            """);
        sources.put("com.example.recursion.CompleteService", "package com.example.recursion; import com.example.recursion.domain.*; public class CompleteService implements org.pipelineframework.service.ReactiveService<Complete, Result> { public io.smallrye.mutiny.Uni<Result> process(Complete input) { return io.smallrye.mutiny.Uni.createFrom().item(new Result(\"done\")); } }");
        sources.put("com.example.recursion.UnwindService", "package com.example.recursion; import com.example.recursion.domain.*; public class UnwindService implements org.pipelineframework.service.ReactiveService<Result, Result> { public static int calls; public static Object context; public io.smallrye.mutiny.Uni<Result> process(Result input) { calls++; context = org.pipelineframework.context.PipelineContextHolder.get(); return io.smallrye.mutiny.Uni.createFrom().item(input); } }");
        return sources;
    }

    private record Fixture(Path projectRoot, Path generatedRoot, Compilation compilation) {
        Path generatedClass(String simpleName) throws IOException {
            try (var stream = Files.walk(generatedRoot)) {
                return stream.filter(path -> path.getFileName() != null
                        && path.getFileName().toString().equals(simpleName + ".java"))
                    .findFirst().orElseThrow(() -> new IllegalStateException("Missing generated class " + simpleName));
            }
        }

        String metadata(String name) throws IOException {
            return compilation.generatedFile(StandardLocation.CLASS_OUTPUT, "META-INF/pipeline", name)
                .orElseThrow(() -> new IllegalStateException("Missing generated metadata " + name))
                .getCharContent(true).toString();
        }
    }
}
