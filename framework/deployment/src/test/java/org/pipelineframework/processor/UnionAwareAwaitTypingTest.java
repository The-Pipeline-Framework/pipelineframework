package org.pipelineframework.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.tools.JavaFileObject;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UnionAwareAwaitTypingTest {

    @TempDir
    Path tempDir;

    @Test
    void narrowsSingleAcceptedUnionVariantFromRealV3Yaml() throws Exception {
        Fixture fixture = compile("single", yaml("[ClarificationRequired]", "ClarificationProjector"),
            sources("ClarificationRequired", "Prepared"));

        assertThat(fixture.compilation()).succeeded();
        String generated = Files.readString(fixture.generated("ProcessClarifyAwaitClientStep"));
        assertTrue(generated.contains("StepOneToOne<ClarificationRequired, Prepared>"), generated);
        assertTrue(generated.contains("applyOneToOne(ClarificationRequired input)"), generated);
        assertTrue(generated.contains("com.example.await.domain.ClarificationRequired"), generated);
        assertFalse(generated.contains("applyOneToOne(PreparationDecision input)"), generated);
        String branching = fixture.metadata("branching.json");
        assertTrue(branching.contains("ClarificationRequired"), branching);
        assertTrue(branching.contains("Prepared"), branching);
        String contract = fixture.metadata("pipeline-contract.json");
        assertTrue(contract.contains("com.example.await.domain.ClarificationRequired"), contract);
        assertTrue(contract.contains("com.example.await.domain.Prepared"), contract);
    }

    @Test
    void keepsDeclaredUnionWhenMultipleVariantsAreAccepted() throws Exception {
        Fixture fixture = compile("multiple",
            yaml("[Prepared, ClarificationRequired]", "DecisionProjector"),
            sources("PreparationDecision", "Prepared"));

        assertThat(fixture.compilation()).succeeded();
        String generated = Files.readString(fixture.generated("ProcessClarifyAwaitClientStep"));
        assertTrue(generated.contains("StepOneToOne<PreparationDecision, Prepared>"), generated);
        assertTrue(generated.contains("applyOneToOne(PreparationDecision input)"), generated);
    }

    @Test
    void acceptsExplicitJavaTypesThatMatchTheCompilerResolvedBoundary() throws Exception {
        String yaml = yaml("[ClarificationRequired]", "ClarificationProjector")
            .replace("    output: Prepared\n", """
                    output: Prepared
                    java:
                      input: com.example.await.domain.ClarificationRequired
                      output: com.example.await.domain.Prepared
                """);

        Compilation compilation = compile("explicit-match", yaml,
            sources("ClarificationRequired", "Prepared")).compilation();

        assertThat(compilation).succeeded();
    }

    @Test
    void rejectsExplicitJavaInputThatConflictsWithTheNarrowedBoundary() throws Exception {
        String yaml = yaml("[ClarificationRequired]", "ClarificationProjector")
            .replace("    output: Prepared\n", """
                    output: Prepared
                    java:
                      input: com.example.await.domain.PreparationDecision
                      output: com.example.await.domain.Prepared
                """);

        Compilation compilation = compile("explicit-mismatch", yaml,
            sources("ClarificationRequired", "Prepared")).compilation();

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining("explicit java.input type 'com.example.await.domain.PreparationDecision'");
        assertThat(compilation).hadErrorContaining(
            "does not match compiler-inferred v3 input type 'com.example.await.domain.ClarificationRequired'");
    }

    @Test
    void rejectsExplicitJavaOutputThatConflictsWithTheSemanticOutput() throws Exception {
        String yaml = yaml("[ClarificationRequired]", "WrongOutputProjector")
            .replace("    output: Prepared\n", """
                    output: Prepared
                    java:
                      input: com.example.await.domain.ClarificationRequired
                      output: com.example.await.domain.ClarificationRequired
                """);

        Compilation compilation = compile("explicit-output-mismatch", yaml,
            sources("ClarificationRequired", "ClarificationRequired")).compilation();

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining(
            "explicit java.output type 'com.example.await.domain.ClarificationRequired'");
        assertThat(compilation).hadErrorContaining(
            "does not match compiler-inferred v3 output type 'com.example.await.domain.Prepared'");
    }

    @Test
    void rejectsProjectorRequestTypeThatDoesNotMatchAcceptedVariant() throws Exception {
        Compilation compilation = compile("bad-input", yaml("[ClarificationRequired]", "DecisionProjector"),
            sources("PreparationDecision", "Prepared")).compilation();

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining("generic I expected 'com.example.await.domain.ClarificationRequired'");
        assertThat(compilation).hadErrorContaining("but was 'com.example.await.domain.PreparationDecision'");
    }

    @Test
    void rejectsProjectorOutputOutsideDeclaredV3Output() throws Exception {
        Compilation compilation = compile("bad-output", yaml("[ClarificationRequired]", "WrongOutputProjector"),
            sources("ClarificationRequired", "ClarificationRequired")).compilation();

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining("generic O expected 'com.example.await.domain.Prepared'");
        assertThat(compilation).hadErrorContaining("but was 'com.example.await.domain.ClarificationRequired'");
    }

    @Test
    void resolvesInheritedProjectorGenericArguments() throws Exception {
        Fixture fixture = compile("inherited", yaml("[ClarificationRequired]", "InheritedProjector"),
            sources("ClarificationRequired", "Prepared"));

        assertThat(fixture.compilation()).succeeded();
        assertTrue(Files.readString(fixture.generated("ProcessClarifyAwaitClientStep"))
            .contains("applyOneToOne(ClarificationRequired input)"));
    }

    @Test
    void rejectsRawProjectorContract() throws Exception {
        Compilation compilation = compile("raw", yaml("[ClarificationRequired]", "RawProjector"),
            sources("ClarificationRequired", "Prepared")).compilation();

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining(
            "must declare concrete, non-parameterized AwaitCompletionProjector<I, C, O> arguments");
    }

    @Test
    void rejectsProjectorCompletionTypeThatDoesNotMatchAwaitContract() throws Exception {
        Compilation compilation = compile("bad-completion",
            yaml("[ClarificationRequired]", "WrongCompletionProjector"),
            sources("ClarificationRequired", "Prepared")).compilation();

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining("generic C expected 'com.example.await.ClarificationAnswer'");
        assertThat(compilation).hadErrorContaining("but was 'java.lang.String'");
    }

    @Test
    void rejectsProjectorWithoutPublicNoArgumentConstructor() throws Exception {
        Compilation compilation = compile("bad-constructor",
            yaml("[ClarificationRequired]", "PrivateConstructorProjector"),
            sources("ClarificationRequired", "Prepared")).compilation();

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining("must declare a public no-argument constructor");
    }

    private Fixture compile(String id, String yaml, Map<String, String> sources) throws IOException {
        Path root = tempDir.resolve(id);
        Path generated = root.resolve("target/generated-sources/pipeline");
        Files.createDirectories(generated);
        Path config = root.resolve("pipeline.yaml");
        Files.writeString(config, yaml);
        List<JavaFileObject> files = sources.entrySet().stream()
            .map(entry -> JavaFileObjects.forSourceString(entry.getKey(), entry.getValue()))
            .toList();
        Compilation compilation = Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Apipeline.config=" + config.toString().replace('\\', '/'),
                "-Apipeline.generatedSourcesDir=" + generated.toString().replace('\\', '/'),
                "-Apipeline.transport=LOCAL")
            .compile(files);
        return new Fixture(generated, compilation);
    }

    private String yaml(String accepts, String projector) {
        return """
            version: 3
            appName: Union-aware Await
            basePackage: com.example.await
            transport: LOCAL
            contract: { input: Request, output: Result }
            types:
              Request: { fields: [[id, string]] }
              Prepared: { fields: [[id, string]] }
              ClarificationRequired: { fields: [[id, string]] }
              PreparationDecision:
                variants: { prepared: Prepared, clarification: ClarificationRequired }
              Result: { fields: [[id, string]] }
            steps:
              - { name: Prepare, service: com.example.await.PrepareService, cardinality: ONE_TO_ONE, input: Request, output: PreparationDecision, java: { input: com.example.await.domain.Request, output: com.example.await.domain.PreparationDecision } }
              - name: Clarify
                kind: await
                cardinality: ONE_TO_ONE
                input: PreparationDecision
                accepts: %s
                output: Prepared
                timeout: PT8H
                await:
                  correlation: { strategy: interactionId }
                  completion:
                    type: com.example.await.ClarificationAnswer
                    projector: com.example.await.%s
                  transport: { type: interaction-api }
              - { name: Finish, service: com.example.await.FinishService, cardinality: ONE_TO_ONE, input: Prepared, output: Result, terminal: true, java: { input: com.example.await.domain.Prepared, output: com.example.await.domain.Result } }
            """.formatted(accepts, projector);
    }

    private Map<String, String> sources(String projectorInput, String projectorOutput) {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put("com.example.await.domain.Request",
            "package com.example.await.domain; public record Request(String id) { }");
        sources.put("com.example.await.domain.PreparationDecision",
            "package com.example.await.domain; public sealed interface PreparationDecision permits Prepared, ClarificationRequired { String id(); }");
        sources.put("com.example.await.domain.Prepared",
            "package com.example.await.domain; public record Prepared(String id) implements PreparationDecision { }");
        sources.put("com.example.await.domain.ClarificationRequired",
            "package com.example.await.domain; public record ClarificationRequired(String id) implements PreparationDecision { }");
        sources.put("com.example.await.domain.Result",
            "package com.example.await.domain; public record Result(String id) { }");
        sources.put("com.example.await.ClarificationAnswer",
            "package com.example.await; public record ClarificationAnswer(String text) { }");
        sources.put("com.example.await.PrepareService", """
            package com.example.await;
            import com.example.await.domain.*;
            public class PrepareService implements org.pipelineframework.service.ReactiveService<Request, PreparationDecision> {
              public io.smallrye.mutiny.Uni<PreparationDecision> process(Request input) {
                return io.smallrye.mutiny.Uni.createFrom().item(new ClarificationRequired(input.id()));
              }
            }
            """);
        sources.put("com.example.await.FinishService", """
            package com.example.await;
            import com.example.await.domain.*;
            public class FinishService implements org.pipelineframework.service.ReactiveService<Prepared, Result> {
              public io.smallrye.mutiny.Uni<Result> process(Prepared input) {
                return io.smallrye.mutiny.Uni.createFrom().item(new Result(input.id()));
              }
            }
            """);
        sources.put("com.example.await.ClarificationProjector",
            projector("ClarificationProjector", projectorInput, projectorOutput));
        sources.put("com.example.await.DecisionProjector",
            projector("DecisionProjector", projectorInput, projectorOutput));
        sources.put("com.example.await.WrongOutputProjector",
            projector("WrongOutputProjector", projectorInput, projectorOutput));
        sources.put("com.example.await.BaseProjector", """
            package com.example.await;
            public abstract class BaseProjector<I, O>
                implements org.pipelineframework.awaitable.AwaitCompletionProjector<I, ClarificationAnswer, O> { }
            """);
        sources.put("com.example.await.InheritedProjector", """
            package com.example.await;
            import com.example.await.domain.*;
            public class InheritedProjector extends BaseProjector<ClarificationRequired, Prepared> {
              public InheritedProjector() { }
              public Prepared project(ClarificationRequired request, ClarificationAnswer completion,
                  org.pipelineframework.awaitable.AwaitCompletionMetadata metadata) {
                return new Prepared(request.id());
              }
            }
            """);
        sources.put("com.example.await.RawProjector", """
            package com.example.await;
            @SuppressWarnings("raw")
            public class RawProjector implements org.pipelineframework.awaitable.AwaitCompletionProjector {
              public RawProjector() { }
              public Object project(Object request, Object completion,
                  org.pipelineframework.awaitable.AwaitCompletionMetadata metadata) { return request; }
            }
            """);
        sources.put("com.example.await.WrongCompletionProjector", """
            package com.example.await;
            import com.example.await.domain.*;
            public class WrongCompletionProjector
                implements org.pipelineframework.awaitable.AwaitCompletionProjector<ClarificationRequired, String, Prepared> {
              public WrongCompletionProjector() { }
              public Prepared project(ClarificationRequired request, String completion,
                  org.pipelineframework.awaitable.AwaitCompletionMetadata metadata) { return new Prepared(request.id()); }
            }
            """);
        sources.put("com.example.await.PrivateConstructorProjector", """
            package com.example.await;
            import com.example.await.domain.*;
            public class PrivateConstructorProjector
                implements org.pipelineframework.awaitable.AwaitCompletionProjector<ClarificationRequired, ClarificationAnswer, Prepared> {
              private PrivateConstructorProjector() { }
              public Prepared project(ClarificationRequired request, ClarificationAnswer completion,
                  org.pipelineframework.awaitable.AwaitCompletionMetadata metadata) { return new Prepared(request.id()); }
            }
            """);
        return Map.copyOf(sources);
    }

    private String projector(String name, String input, String output) {
        return """
            package com.example.await;
            import com.example.await.domain.*;
            public class %s implements org.pipelineframework.awaitable.AwaitCompletionProjector<%s, ClarificationAnswer, %s> {
              public %s() { }
              public %s project(%s request, ClarificationAnswer completion,
                  org.pipelineframework.awaitable.AwaitCompletionMetadata metadata) {
                throw new UnsupportedOperationException();
              }
            }
            """.formatted(name, input, output, name, output, input);
    }

    private record Fixture(Path generatedRoot, Compilation compilation) {
        Path generated(String simpleName) throws IOException {
            try (var paths = Files.walk(generatedRoot)) {
                return paths.filter(path -> path.getFileName() != null
                        && path.getFileName().toString().equals(simpleName + ".java"))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Missing generated class " + simpleName));
            }
        }

        String metadata(String name) throws IOException {
            return compilation.generatedFile(javax.tools.StandardLocation.CLASS_OUTPUT, "META-INF/pipeline", name)
                .orElseThrow().getCharContent(true).toString();
        }
    }
}
