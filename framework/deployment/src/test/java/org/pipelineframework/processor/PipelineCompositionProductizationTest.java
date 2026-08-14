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
import java.util.concurrent.atomic.AtomicInteger;
import javax.tools.JavaFileObject;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.PipelineRunner;
import org.pipelineframework.TestPipelineRunnerFactory;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class PipelineCompositionProductizationTest {

    @TempDir
    Path tempDir;

    @Test
    void childUnionAcceptsAndNonAdjacentFlowCompileGenerateAndExecute() throws Exception {
        String yaml = """
            version: 3
            appName: Routed child
            basePackage: com.example.routed
            transport: LOCAL
            contract: { input: Request, output: FinalResult }
            types:
              Request: { fields: [[id, string]] }
              Approved: { fields: [[id, string]] }
              Rejected: { fields: [[id, string]] }
              Outcome:
                variants: { approved: Approved, rejected: Rejected }
              ApprovedHandled: { fields: [[id, string]] }
              RejectedHandled: { fields: [[id, string]] }
              Completion:
                variants: { approved: ApprovedHandled, rejected: RejectedHandled }
              FinalResult: { fields: [[id, string]] }
            pipelines:
              routed:
                input: Request
                output: FinalResult
                steps:
                  - { name: Classify, service: com.example.routed.ClassifyService, cardinality: ONE_TO_ONE, input: Request, output: Outcome, java: { input: com.example.routed.domain.Request, output: com.example.routed.domain.Outcome } }
                  - { name: Handle approved, service: com.example.routed.HandleApprovedService, cardinality: ONE_TO_ONE, input: Outcome, output: ApprovedHandled, accepts: [Approved], java: { input: com.example.routed.domain.Approved, output: com.example.routed.domain.ApprovedHandled } }
                  - { name: Handle rejected, service: com.example.routed.HandleRejectedService, cardinality: ONE_TO_ONE, input: Outcome, output: RejectedHandled, accepts: [Rejected], java: { input: com.example.routed.domain.Rejected, output: com.example.routed.domain.RejectedHandled } }
                  - { name: Finalize, service: com.example.routed.FinalizeService, cardinality: ONE_TO_ONE, input: Completion, output: FinalResult, accepts: [ApprovedHandled, RejectedHandled], terminal: true, java: { input: com.example.routed.domain.Completion, output: com.example.routed.domain.FinalResult } }
            steps:
              - { name: Call routed, pipeline: routed, cardinality: ONE_TO_ONE, input: Request, output: FinalResult, java: { input: com.example.routed.domain.Request, output: com.example.routed.domain.FinalResult } }
            """;
        Map<String, String> sources = routedSources();
        Fixture fixture = compile("routed", yaml, sources);

        assertThat(fixture.compilation()).succeeded();
        Fixture repeated = compile("routed-repeat", yaml, sources);
        assertThat(repeated.compilation()).succeeded();
        String contract = fixture.metadata("pipeline-contract.json");
        assertEquals(contract, repeated.metadata("pipeline-contract.json"),
            "The schema-v3 composition projection and contract hash must be deterministic");
        assertTrue(contract.contains("\"schemaVersion\": 3"));
        assertTrue(contract.contains("\"composition\""));
        assertTrue(contract.contains("\"acceptedContractIds\""));
        assertTrue(contract.contains("\"NEXT_LOCAL\""));
        assertTrue(contract.contains("\"RETURN\""));
        assertTrue(contract.contains("\"ROOT_TERMINAL\""));
        String invocation = Files.readString(fixture.generatedClass("PipelineInvocation_root_Call_routed"));
        assertTrue(invocation.contains("PipelineInvocationSteps.<Request, FinalResult>oneToOne"));
        assertTrue(invocation.contains("java.util.List.of(child0, child1, child2, child3)"));
        assertFalse(invocation.contains("ExecutionRecord"));
        assertFalse(invocation.contains("admission"));
        String branching = fixture.metadata("branching.json");
        assertTrue(branching.contains("Handle approved"));
        assertTrue(branching.contains("Handle rejected"));
        assertTrue(branching.contains("ApprovedHandled"));
        assertFalse(fixture.metadata("order.json").contains("ProcessHandleApprovedLocalClientStep"));

        Path classes = compileGeneratedFixture(fixture, sources,
            List.of("PipelineInvocation_root_Call_routed", "ProcessClassifyLocalClientStep",
                "ProcessHandleApprovedLocalClientStep", "ProcessHandleRejectedLocalClientStep",
                "ProcessFinalizeLocalClientStep"));
        Files.createDirectories(classes.resolve("META-INF/pipeline"));
        Files.writeString(classes.resolve("META-INF/pipeline/branching.json"), branching);

        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(new java.net.URL[]{classes.toUri().toURL()}, previous)) {
            Thread.currentThread().setContextClassLoader(loader);
            TestPipelineRunnerFactory.Harness harness = TestPipelineRunnerFactory.createHarness();
            Object generated = instantiateGeneratedInvocation(loader, harness.runner(),
                "com.example.routed.pipeline.PipelineInvocation_root_Call_routed",
                List.of("com.example.routed.pipeline.ProcessClassifyLocalClientStep",
                    "com.example.routed.pipeline.ProcessHandleApprovedLocalClientStep",
                    "com.example.routed.pipeline.ProcessHandleRejectedLocalClientStep",
                    "com.example.routed.pipeline.ProcessFinalizeLocalClientStep"),
                List.of("com.example.routed.ClassifyService", "com.example.routed.HandleApprovedService",
                    "com.example.routed.HandleRejectedService", "com.example.routed.FinalizeService"));
            Object request = loader.loadClass("com.example.routed.domain.Request").getConstructor(String.class).newInstance("42");
            PipelineContext context = new PipelineContext("release-7", "live", "default");
            AtomicInteger subscriptions = new AtomicInteger();
            PipelineContextHolder.set(context);
            PipelineRunner.ExecutionResult execution;
            Object terminal;
            try {
                execution = harness.runner().runWithContext(
                    Uni.createFrom().deferred(() -> {
                        subscriptions.incrementAndGet();
                        return Uni.createFrom().item(request);
                    }),
                    List.of(generated));
                terminal = ((Uni<?>) execution.result()).await().indefinitely();
            } finally {
                PipelineContextHolder.clear();
            }
            assertEquals("FinalResult[id=42]", terminal.toString());
            assertTrue(execution.terminalOutputPublished(), "Only the root run must own terminal publication");
            assertEquals(1, subscriptions.get(), "Nested invocation must not add a root-source subscription");
            verify(harness.telemetry(), times(1)).startRun(any(), anyInt(), any(), anyInt());
            verify(harness.publisher(), times(1)).publish(any());
            harness.verifyRootOrderAppliedOnce();
            assertSame(context, loader.loadClass("com.example.routed.ClassifyService").getField("context").get(null));
            assertSame(context, loader.loadClass("com.example.routed.HandleApprovedService").getField("context").get(null));
            assertSame(context, loader.loadClass("com.example.routed.FinalizeService").getField("context").get(null));
            assertEquals(1, loader.loadClass("com.example.routed.HandleApprovedService").getField("calls").getInt(null));
            assertEquals(0, loader.loadClass("com.example.routed.HandleRejectedService").getField("calls").getInt(null));
            assertEquals(1, loader.loadClass("com.example.routed.FinalizeService").getField("calls").getInt(null));
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    @Test
    void generatedInvocationUsesCallsiteJavaBoundaryForV3CompatibleUnionFlow() throws Exception {
        String yaml = """
            version: 3
            appName: Compatible invocation boundary
            basePackage: com.example.compatible
            transport: LOCAL
            contract: { input: Choice, output: Result }
            types:
              Payload: { fields: [[id, string]] }
              Choice: { variants: { payload: Payload } }
              Result: { fields: [[id, string]] }
            pipelines:
              payload-handler:
                input: Payload
                output: Result
                steps:
                  - { name: Handle payload, service: com.example.compatible.HandlePayloadService, cardinality: ONE_TO_ONE, input: Payload, output: Result, java: { input: com.example.compatible.domain.Payload, output: com.example.compatible.domain.Result } }
            steps:
              - { name: Call handler, pipeline: payload-handler, cardinality: ONE_TO_ONE, input: Choice, output: Result, accepts: [Payload], terminal: true, java: { input: com.example.compatible.domain.Choice, output: com.example.compatible.domain.Result } }
            """;
        Map<String, String> sources = Map.of(
            "com.example.compatible.domain.Choice",
            "package com.example.compatible.domain; public sealed interface Choice permits Payload { String id(); }",
            "com.example.compatible.domain.Payload",
            "package com.example.compatible.domain; public record Payload(String id) implements Choice { }",
            "com.example.compatible.domain.Result",
            "package com.example.compatible.domain; public record Result(String id) { }",
            "com.example.compatible.HandlePayloadService",
            "package com.example.compatible; import com.example.compatible.domain.*; public class HandlePayloadService implements org.pipelineframework.service.ReactiveService<Payload, Result> { public io.smallrye.mutiny.Uni<Result> process(Payload input) { return io.smallrye.mutiny.Uni.createFrom().item(new Result(input.id())); } }");

        Fixture fixture = compile("compatible-boundary", yaml, sources);

        assertThat(fixture.compilation()).succeeded();
        String invocation = Files.readString(fixture.generatedClass("PipelineInvocation_root_Call_handler"));
        assertTrue(invocation.contains("StepOneToOne<Choice, Result>"));
        assertTrue(invocation.contains("PipelineInvocationSteps.<Choice, Result>oneToOne"));
        Path classes = compileGeneratedFixture(fixture, sources,
            List.of("PipelineInvocation_root_Call_handler", "ProcessHandlePayloadLocalClientStep"));
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(new java.net.URL[]{classes.toUri().toURL()}, previous)) {
            Thread.currentThread().setContextClassLoader(loader);
            Object invocationStep = instantiateGeneratedInvocation(loader,
                "com.example.compatible.pipeline.PipelineInvocation_root_Call_handler",
                List.of("com.example.compatible.pipeline.ProcessHandlePayloadLocalClientStep"),
                List.of("com.example.compatible.HandlePayloadService"));
            Object payload = loader.loadClass("com.example.compatible.domain.Payload")
                .getConstructor(String.class).newInstance("42");
            Object result = ((Uni<?>) invocationStep.getClass()
                .getMethod("applyOneToOne", loader.loadClass("com.example.compatible.domain.Choice"))
                .invoke(invocationStep, payload)).await().indefinitely();
            assertEquals("Result[id=42]", result.toString());
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    @Test
    void generatedInvocationPreservesFrameworkOwnedCommandAndQueryStepRealizations() throws Exception {
        String yaml = """
            version: 3
            appName: Framework owned children
            basePackage: com.example.shell
            transport: LOCAL
            contract: { input: Request, output: Snapshot }
            types:
              Request: { fields: [[id, string]] }
              Written: { fields: [[id, string]] }
              Snapshot: { fields: [[id, string]] }
            queries:
              written-by-id:
                connector: jpa
                input: com.example.shell.Written
                output: com.example.shell.Snapshot
                jpa:
                  entity: com.example.shell.StoredRecord
                  where: { id: input.id }
            pipelines:
              writer:
                input: Request
                output: Written
                steps:
                  - { name: Write item, kind: command, command: test-command, commandIdGenerator: com.example.shell.RequestId, cardinality: ONE_TO_ONE, input: Request, output: Written, java: { input: com.example.shell.Request, output: com.example.shell.Written } }
              reader:
                input: Written
                output: Snapshot
                steps:
                  - { name: Read item, kind: query, query: written-by-id, cardinality: ONE_TO_ONE, input: Written, output: Snapshot, capture: { keyFields: [id] }, java: { input: com.example.shell.Written, output: com.example.shell.Snapshot } }
            steps:
              - { name: Call writer, pipeline: writer, cardinality: ONE_TO_ONE, input: Request, output: Written, java: { input: com.example.shell.Request, output: com.example.shell.Written } }
              - { name: Call reader, pipeline: reader, cardinality: ONE_TO_ONE, input: Written, output: Snapshot, java: { input: com.example.shell.Written, output: com.example.shell.Snapshot } }
            """;
        Map<String, String> sources = Map.of(
            "com.example.shell.Request", "package com.example.shell; public record Request(String id) { }",
            "com.example.shell.Written", "package com.example.shell; public record Written(String id) { }",
            "com.example.shell.Snapshot", "package com.example.shell; public record Snapshot(String id) { }",
            "com.example.shell.RequestId", "package com.example.shell; public class RequestId implements org.pipelineframework.command.CommandIdGenerator<Request> { public String commandId(org.pipelineframework.command.CommandDescriptor descriptor, Request input) { return input.id(); } }");

        Fixture fixture = compile("framework-owned-children", yaml, sources);

        assertThat(fixture.compilation()).succeeded();
        String writer = Files.readString(fixture.generatedClass("PipelineInvocation_root_Call_writer"));
        String reader = Files.readString(fixture.generatedClass("PipelineInvocation_root_Call_reader"));
        assertTrue(writer.contains("ProcessWriteItemCommandClientStep child0"));
        assertFalse(writer.contains("ProcessWriteItemLocalClientStep"));
        assertTrue(reader.contains("ProcessReadItemQueryClientStep child0"));
        assertFalse(reader.contains("ProcessReadItemLocalClientStep"));
        String order = fixture.metadata("order.json");
        assertTrue(order.contains("PipelineInvocation_root_Call_writer"));
        assertTrue(order.contains("PipelineInvocation_root_Call_reader"));
        assertFalse(order.contains("CommandClientStep"));
        assertFalse(order.contains("QueryClientStep"));
    }

    @Test
    void generatedInvocationSelectsAllStreamingCardinalityAdapters() throws Exception {
        String yaml = cardinalityYaml();
        Map<String, String> sources = cardinalitySources();
        Fixture fixture = compile("cardinalities", yaml, sources);

        assertThat(fixture.compilation()).succeeded();
        assertTrue(Files.readString(fixture.generatedClass("PipelineInvocation_root_Pointwise"))
            .contains("PipelineInvocationSteps.<Value, Value>oneToOne"));
        assertTrue(Files.readString(fixture.generatedClass("PipelineInvocation_root_Expand"))
            .contains("PipelineInvocationSteps.<Value, Value>oneToMany"));
        assertTrue(Files.readString(fixture.generatedClass("PipelineInvocation_root_Reduce"))
            .contains("PipelineInvocationSteps.<Value, Value>manyToOne"));
        assertTrue(Files.readString(fixture.generatedClass("PipelineInvocation_root_Transform"))
            .contains("PipelineInvocationSteps.<Value, Value>manyToMany"));

        Path classes = compileGeneratedFixture(fixture, sources,
            List.of("PipelineInvocation_root_Pointwise", "PipelineInvocation_root_Expand", "PipelineInvocation_root_Reduce",
                "PipelineInvocation_root_Transform", "ProcessSplitLocalClientStep",
                "ProcessIdentityLocalClientStep", "ProcessJoinLocalClientStep", "ProcessMapStreamLocalClientStep"));
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(new java.net.URL[]{classes.toUri().toURL()}, previous)) {
            Thread.currentThread().setContextClassLoader(loader);
            Class<?> valueType = loader.loadClass("com.example.cardinality.Value");
            Object value = valueType.getConstructor(String.class).newInstance("v");

            TestPipelineRunnerFactory.Harness pointwiseHarness = TestPipelineRunnerFactory.createHarness();
            Object pointwise = instantiateGeneratedInvocation(loader, pointwiseHarness.runner(),
                "com.example.cardinality.pipeline.PipelineInvocation_root_Pointwise",
                List.of("com.example.cardinality.pipeline.ProcessIdentityLocalClientStep"),
                List.of("com.example.cardinality.IdentityService"));
            AtomicInteger rootSubscriptions = new AtomicInteger();
            Multi<?> pointwiseInput = Multi.createFrom().deferred(() -> {
                rootSubscriptions.incrementAndGet();
                return Multi.createFrom().items(value, value, value, value, value);
            });
            Multi<?> pointwiseOutput = (Multi<?>) pointwiseHarness.runner()
                .runWithContext(pointwiseInput, List.of(pointwise)).result();
            assertEquals(5, pointwiseOutput.collect().asList().await().indefinitely().size());
            assertEquals(1, rootSubscriptions.get());
            assertEquals(5, loader.loadClass("com.example.cardinality.IdentityService").getField("calls").getInt(null));
            assertEquals(1, loader.loadClass("com.example.cardinality.IdentityService").getField("maxActive").getInt(null),
                "Generated pointwise invocation over Multi must honor the existing maxConcurrency bound");

            Object expand = instantiateGeneratedInvocation(loader,
                "com.example.cardinality.pipeline.PipelineInvocation_root_Expand",
                List.of("com.example.cardinality.pipeline.ProcessSplitLocalClientStep"),
                List.of("com.example.cardinality.SplitService"));
            Multi<?> expanded = (Multi<?>) expand.getClass().getMethod("applyOneToMany", valueType)
                .invoke(expand, value);
            assertEquals(List.of("Value[id=v]", "Value[id=v]"),
                expanded.map(Object::toString).collect().asList().await().indefinitely());

            Object reduce = instantiateGeneratedInvocation(loader,
                "com.example.cardinality.pipeline.PipelineInvocation_root_Reduce",
                List.of("com.example.cardinality.pipeline.ProcessJoinLocalClientStep"),
                List.of("com.example.cardinality.JoinService"));
            Uni<?> reduced = (Uni<?>) reduce.getClass().getMethod("apply", Multi.class)
                .invoke(reduce, Multi.createFrom().items(value, value));
            assertEquals("Value[id=v]", reduced.await().indefinitely().toString());
            assertEquals(1, loader.loadClass("com.example.cardinality.JoinService").getField("calls").getInt(null),
                "A stream-scoped child must be invoked once for the parent stream");

            Object transform = instantiateGeneratedInvocation(loader,
                "com.example.cardinality.pipeline.PipelineInvocation_root_Transform",
                List.of("com.example.cardinality.pipeline.ProcessMapStreamLocalClientStep"),
                List.of("com.example.cardinality.TransformService"));
            Multi<?> transformed = (Multi<?>) transform.getClass().getMethod("applyTransform", Multi.class)
                .invoke(transform, Multi.createFrom().items(value, value));
            assertEquals(List.of("Value[id=v]", "Value[id=v]"),
                transformed.map(Object::toString).collect().asList().await().indefinitely());
            assertEquals(1, loader.loadClass("com.example.cardinality.TransformService").getField("calls").getInt(null),
                "A MANY_TO_MANY child must receive the parent stream once");
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    @Test
    void rejectsUnknownPipelineReferenceWithCallsiteDiagnostic() throws Exception {
        Compilation compilation = compile("unknown-reference", diagnosticYaml("{}", """
            - { name: Call missing, pipeline: missing, cardinality: ONE_TO_ONE, input: A, output: A, java: { input: com.example.diagnostic.A, output: com.example.diagnostic.A } }
            """), diagnosticSources()).compilation();

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining("Static pipeline reference could not be resolved: missing");
    }

    @Test
    void rejectsDirectAndTransitiveStaticCyclesWithDefinitionPath() throws Exception {
        Compilation direct = compile("direct-cycle", diagnosticYaml("""
            loop:
              input: A
              output: A
              steps:
                - { name: Recur, pipeline: loop, cardinality: ONE_TO_ONE, input: A, output: A, java: { input: com.example.diagnostic.A, output: com.example.diagnostic.A } }
            """, """
            - { name: Call loop, pipeline: loop, cardinality: ONE_TO_ONE, input: A, output: A, java: { input: com.example.diagnostic.A, output: com.example.diagnostic.A } }
            """), diagnosticSources()).compilation();
        assertThat(direct).failed();
        assertThat(direct).hadErrorContaining("Static pipeline definition cycle is not supported: loop -> loop");

        Compilation transitive = compile("transitive-cycle", diagnosticYaml("""
            first:
              input: A
              output: A
              steps:
                - { name: Call second, pipeline: second, cardinality: ONE_TO_ONE, input: A, output: A, java: { input: com.example.diagnostic.A, output: com.example.diagnostic.A } }
            second:
              input: A
              output: A
              steps:
                - { name: Call first, pipeline: first, cardinality: ONE_TO_ONE, input: A, output: A, java: { input: com.example.diagnostic.A, output: com.example.diagnostic.A } }
            """, """
            - { name: Enter first, pipeline: first, cardinality: ONE_TO_ONE, input: A, output: A, java: { input: com.example.diagnostic.A, output: com.example.diagnostic.A } }
            """), diagnosticSources()).compilation();
        assertThat(transitive).failed();
        assertThat(transitive).hadErrorContaining(
            "Static pipeline definition cycle is not supported: first -> second -> first");
    }

    @Test
    void rejectsInvocationInputAndOutputIncompatibilityUsingV3Compatibility() throws Exception {
        Compilation input = compile("input-incompatible", diagnosticYaml("""
            inner:
              input: B
              output: A
              steps:
                - { name: Convert, service: com.example.diagnostic.BToAService, cardinality: ONE_TO_ONE, input: B, output: A, java: { input: com.example.diagnostic.B, output: com.example.diagnostic.A } }
            """, """
            - { name: Call inner, pipeline: inner, cardinality: ONE_TO_ONE, input: A, output: A, java: { input: com.example.diagnostic.A, output: com.example.diagnostic.A } }
            """), diagnosticSources()).compilation();
        assertThat(input).failed();
        assertThat(input).hadErrorContaining(
            "Pipeline reference inner input contract does not match callsite Call inner");

        Compilation output = compile("output-incompatible", diagnosticYaml("""
            inner:
              input: A
              output: B
              steps:
                - { name: Convert, service: com.example.diagnostic.AToBService, cardinality: ONE_TO_ONE, input: A, output: B, java: { input: com.example.diagnostic.A, output: com.example.diagnostic.B } }
            """, """
            - { name: Call inner, pipeline: inner, cardinality: ONE_TO_ONE, input: A, output: A, java: { input: com.example.diagnostic.A, output: com.example.diagnostic.A } }
            """), diagnosticSources()).compilation();
        assertThat(output).failed();
        assertThat(output).hadErrorContaining(
            "Pipeline reference inner output contract does not match callsite Call inner");
    }

    @Test
    void rejectsInvalidChildRoutingThroughExistingBranchPlanner() throws Exception {
        Compilation compilation = compile("invalid-routing", diagnosticYaml("""
            inner:
              input: A
              output: A
              steps:
                - { name: Classify, service: com.example.diagnostic.ClassifyService, cardinality: ONE_TO_ONE, input: A, output: Choice, java: { input: com.example.diagnostic.A, output: com.example.diagnostic.Choice } }
                - { name: Handle B, service: com.example.diagnostic.BToAService, cardinality: ONE_TO_ONE, input: Choice, output: A, accepts: [B], java: { input: com.example.diagnostic.B, output: com.example.diagnostic.A } }
            """, """
            - { name: Call inner, pipeline: inner, cardinality: ONE_TO_ONE, input: A, output: A, java: { input: com.example.diagnostic.A, output: com.example.diagnostic.A } }
            """), diagnosticSources()).compilation();

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining("exactly one step with terminal: true");
    }

    @Test
    void rejectsNestedAwaitAsExplicitSliceTwoBoundary() throws Exception {
        Compilation compilation = compile("nested-await", diagnosticYaml("""
            inner:
              input: A
              output: A
              steps:
                - { name: Wait, kind: await, cardinality: ONE_TO_ONE, input: A, output: A, java: { input: com.example.diagnostic.A, output: com.example.diagnostic.A } }
            """, """
            - { name: Call inner, pipeline: inner, cardinality: ONE_TO_ONE, input: A, output: A, java: { input: com.example.diagnostic.A, output: com.example.diagnostic.A } }
            """), diagnosticSources()).compilation();

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining(
            "Pipeline definition 'inner' contains kind: await; nested Await is not supported in this slice");
    }

    private Object instantiateGeneratedInvocation(
        ClassLoader loader,
        String invocationClass,
        List<String> childClasses,
        List<String> serviceClasses
    ) throws Exception {
        return instantiateGeneratedInvocation(loader, TestPipelineRunnerFactory.create(), invocationClass,
            childClasses, serviceClasses);
    }

    private Object instantiateGeneratedInvocation(
        ClassLoader loader,
        PipelineRunner runner,
        String invocationClass,
        List<String> childClasses,
        List<String> serviceClasses
    ) throws Exception {
        Object invocation = loader.loadClass(invocationClass).getConstructor().newInstance();
        set(invocation, "runner", runner);
        for (int index = 0; index < childClasses.size(); index++) {
            Object child = loader.loadClass(childClasses.get(index)).getConstructor().newInstance();
            Object service = loader.loadClass(serviceClasses.get(index)).getConstructor().newInstance();
            set(child, "service", service);
            set(invocation, "child" + index, child);
        }
        return invocation;
    }

    private static void set(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
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
        try (var fileManager = compiler.getStandardFileManager(null, null, null)) {
            var units = fileManager.getJavaFileObjectsFromPaths(javaSources);
            boolean success = compiler.getTask(null, fileManager, null,
                List.of("-proc:none", "-classpath", System.getProperty("java.class.path"), "-d", classes.toString()),
                null, units).call();
            assertTrue(success, "Generated invocation and child client sources must compile against runtime artifacts");
        }
        return classes;
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

    private Map<String, String> routedSources() {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("com.example.routed.domain.Request", "package com.example.routed.domain; public record Request(String id) { }");
        sources.put("com.example.routed.domain.Outcome", "package com.example.routed.domain; public sealed interface Outcome permits Approved, Rejected { String id(); }");
        sources.put("com.example.routed.domain.Approved", "package com.example.routed.domain; public record Approved(String id) implements Outcome { }");
        sources.put("com.example.routed.domain.Rejected", "package com.example.routed.domain; public record Rejected(String id) implements Outcome { }");
        sources.put("com.example.routed.domain.Completion", "package com.example.routed.domain; public sealed interface Completion permits ApprovedHandled, RejectedHandled { String id(); }");
        sources.put("com.example.routed.domain.ApprovedHandled", "package com.example.routed.domain; public record ApprovedHandled(String id) implements Completion { }");
        sources.put("com.example.routed.domain.RejectedHandled", "package com.example.routed.domain; public record RejectedHandled(String id) implements Completion { }");
        sources.put("com.example.routed.domain.FinalResult", "package com.example.routed.domain; public record FinalResult(String id) { }");
        sources.put("com.example.routed.ClassifyService", unaryService("ClassifyService", "Request", "Outcome", "new Approved(input.id())", false));
        sources.put("com.example.routed.HandleApprovedService", unaryService("HandleApprovedService", "Approved", "ApprovedHandled", "new ApprovedHandled(input.id())", true));
        sources.put("com.example.routed.HandleRejectedService", unaryService("HandleRejectedService", "Rejected", "RejectedHandled", "new RejectedHandled(input.id())", true));
        sources.put("com.example.routed.FinalizeService", unaryService("FinalizeService", "Completion", "FinalResult", "new FinalResult(input.id())", true));
        return Map.copyOf(sources);
    }

    private String unaryService(String name, String input, String output, String expression, boolean counter) {
        return """
            package com.example.routed;
            import com.example.routed.domain.*;
            import io.smallrye.mutiny.Uni;
            import org.pipelineframework.service.ReactiveService;
            public class %s implements ReactiveService<%s, %s> {
              public static int calls;
              public static Object context;
              public Uni<%s> process(%s input) { context = org.pipelineframework.context.PipelineContextHolder.get(); %s return Uni.createFrom().item(%s); }
            }
            """.formatted(name, input, output, output, input, counter ? "calls++;" : "", expression);
    }

    private String cardinalityYaml() {
        return """
            version: 3
            appName: Cardinalities
            basePackage: com.example.cardinality
            transport: LOCAL
            contract: { input: Value, output: Value }
            types: { Value: { fields: [[id, string]] } }
            pipelines:
              pointwise:
                input: Value
                output: Value
                steps: [{ name: Identity, service: com.example.cardinality.IdentityService, cardinality: ONE_TO_ONE, input: Value, output: Value, java: { input: com.example.cardinality.Value, output: com.example.cardinality.Value } }]
              expand:
                input: Value
                output: Value
                steps: [{ name: Split, service: com.example.cardinality.SplitService, cardinality: ONE_TO_MANY, input: Value, output: Value, java: { input: com.example.cardinality.Value, output: com.example.cardinality.Value } }]
              reduce:
                input: Value
                output: Value
                steps: [{ name: Join, service: com.example.cardinality.JoinService, cardinality: MANY_TO_ONE, input: Value, output: Value, java: { input: com.example.cardinality.Value, output: com.example.cardinality.Value } }]
              transform:
                input: Value
                output: Value
                steps: [{ name: Map stream, service: com.example.cardinality.TransformService, cardinality: MANY_TO_MANY, input: Value, output: Value, java: { input: com.example.cardinality.Value, output: com.example.cardinality.Value } }]
            steps:
              - { name: Pointwise, pipeline: pointwise, cardinality: ONE_TO_ONE, input: Value, output: Value, java: { input: com.example.cardinality.Value, output: com.example.cardinality.Value } }
              - { name: Expand, pipeline: expand, cardinality: ONE_TO_MANY, input: Value, output: Value, java: { input: com.example.cardinality.Value, output: com.example.cardinality.Value } }
              - { name: Reduce, pipeline: reduce, cardinality: MANY_TO_ONE, input: Value, output: Value, java: { input: com.example.cardinality.Value, output: com.example.cardinality.Value } }
              - { name: Transform, pipeline: transform, cardinality: MANY_TO_MANY, input: Value, output: Value, java: { input: com.example.cardinality.Value, output: com.example.cardinality.Value } }
            """;
    }

    private Map<String, String> cardinalitySources() {
        return Map.of(
            "com.example.cardinality.Value", "package com.example.cardinality; public record Value(String id) { }",
            "com.example.cardinality.IdentityService", "package com.example.cardinality; public class IdentityService implements org.pipelineframework.service.ReactiveService<Value, Value> { public static int calls; public static int active; public static int maxActive; public io.smallrye.mutiny.Uni<Value> process(Value input) { calls++; active++; maxActive = Math.max(maxActive, active); return io.smallrye.mutiny.Uni.createFrom().item(input).onItem().delayIt().by(java.time.Duration.ofMillis(10)).onTermination().invoke(() -> active--); } }",
            "com.example.cardinality.SplitService", "package com.example.cardinality; public class SplitService implements org.pipelineframework.service.ReactiveStreamingService<Value, Value> { public io.smallrye.mutiny.Multi<Value> process(Value input) { return io.smallrye.mutiny.Multi.createFrom().items(input, input); } }",
            "com.example.cardinality.JoinService", "package com.example.cardinality; public class JoinService implements org.pipelineframework.service.ReactiveStreamingClientService<Value, Value> { public static int calls; public io.smallrye.mutiny.Uni<Value> process(io.smallrye.mutiny.Multi<Value> input) { calls++; return input.collect().first(); } }",
            "com.example.cardinality.TransformService", "package com.example.cardinality; public class TransformService implements org.pipelineframework.service.ReactiveBidirectionalStreamingService<Value, Value> { public static int calls; public io.smallrye.mutiny.Multi<Value> process(io.smallrye.mutiny.Multi<Value> input) { calls++; return input; } }"
        );
    }

    private String diagnosticYaml(String pipelines, String rootSteps) {
        return """
            version: 3
            appName: Diagnostics
            basePackage: com.example.diagnostic
            transport: LOCAL
            contract: { input: A, output: A }
            types:
              A: { fields: [[id, string]] }
              B: { fields: [[id, string]] }
              C: { fields: [[id, string]] }
              Choice: { variants: { b: B, c: C } }
            pipelines:
            %s
            steps:
            %s
            """.formatted(indent(pipelines, 2), indent(rootSteps, 2))
            .replace("input: com.example.diagnostic.", "input: com.example.diagnostic.domain.")
            .replace("output: com.example.diagnostic.", "output: com.example.diagnostic.domain.");
    }

    private String indent(String value, int spaces) {
        String prefix = " ".repeat(spaces);
        return value.lines().map(prefix::concat).collect(java.util.stream.Collectors.joining("\n"));
    }

    private Map<String, String> diagnosticSources() {
        return Map.of(
            "com.example.diagnostic.domain.A", "package com.example.diagnostic.domain; public record A(String id) { }",
            "com.example.diagnostic.domain.B", "package com.example.diagnostic.domain; public record B(String id) implements Choice { }",
            "com.example.diagnostic.domain.C", "package com.example.diagnostic.domain; public record C(String id) implements Choice { }",
            "com.example.diagnostic.domain.Choice", "package com.example.diagnostic.domain; public sealed interface Choice permits B, C { String id(); }",
            "com.example.diagnostic.BToAService", "package com.example.diagnostic; import com.example.diagnostic.domain.*; public class BToAService implements org.pipelineframework.service.ReactiveService<B, A> { public io.smallrye.mutiny.Uni<A> process(B input) { return io.smallrye.mutiny.Uni.createFrom().item(new A(input.id())); } }",
            "com.example.diagnostic.AToBService", "package com.example.diagnostic; import com.example.diagnostic.domain.*; public class AToBService implements org.pipelineframework.service.ReactiveService<A, B> { public io.smallrye.mutiny.Uni<B> process(A input) { return io.smallrye.mutiny.Uni.createFrom().item(new B(input.id())); } }",
            "com.example.diagnostic.ClassifyService", "package com.example.diagnostic; import com.example.diagnostic.domain.*; public class ClassifyService implements org.pipelineframework.service.ReactiveService<A, Choice> { public io.smallrye.mutiny.Uni<Choice> process(A input) { return io.smallrye.mutiny.Uni.createFrom().item(new B(input.id())); } }"
        );
    }

    private record Fixture(Path projectRoot, Path generatedRoot, Compilation compilation) {
        Path generatedClass(String simpleName) throws IOException {
            try (var stream = Files.walk(generatedRoot)) {
                return stream.filter(path -> path.getFileName() != null
                        && path.getFileName().toString().equals(simpleName + ".java"))
                    .findFirst().orElseThrow(() -> new IllegalStateException("Missing generated class " + simpleName
                        + "; generated=" + generatedFiles()));
            }
        }

        private List<String> generatedFiles() {
            try (var stream = Files.walk(generatedRoot)) {
                return stream.filter(path -> path.toString().endsWith(".java"))
                    .map(generatedRoot::relativize).map(Path::toString).sorted().toList();
            } catch (IOException exception) {
                return List.of("<failed to list: " + exception.getMessage() + ">");
            }
        }

        String metadata(String name) throws IOException {
            return compilation.generatedFile(StandardLocation.CLASS_OUTPUT, "META-INF/pipeline", name)
                .orElseThrow(() -> new IllegalStateException("Missing generated metadata " + name))
                .getCharContent(true).toString();
        }
    }
}
