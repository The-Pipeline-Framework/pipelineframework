package org.pipelineframework.processor;

import java.nio.file.Files;
import java.nio.file.Path;
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
    @TempDir Path tempDir;

    @Test
    void decoratesNarrowedAuthoredOperationAndExposesFinalOutput() throws Exception {
        Fixture fixture = compile("success", yaml("ClarificationProjector"), sources(false, false));

        assertThat(fixture.compilation()).succeeded();
        String operation = Files.readString(fixture.generated("ProcessClarifyLocalClientStep"));
        String generated = Files.readString(fixture.generated("ProcessClarifyDeferredCompletionStep"));
        assertTrue(operation.contains("StepOneToOne<ClarificationRequired, PendingClarification>"), operation);
        assertTrue(generated.contains("StepOneToOne<PendingClarification, Prepared>"), generated);
        assertFalse(generated.contains("ProcessClarifyLocalClientStep operation"), generated);
        assertTrue(generated.contains("AwaitCompletionDescriptor"), generated);
        assertTrue(generated.contains("com.example.await.domain.PendingClarification"), generated);
        String contract = fixture.metadata("pipeline-contract.json");
        assertTrue(contract.contains("\"kind\": \"internal\""), contract);
        assertTrue(contract.contains("\"deferredCompletion\""), contract);
        assertTrue(contract.contains("com.example.await.domain.PendingClarification"), contract);
        assertTrue(contract.contains("com.example.await.domain.Prepared"), contract);
    }

    @Test
    void validatesProjectorRequestAgainstOperationOutput() throws Exception {
        Compilation compilation = compile("bad-input", yaml("WrongInputProjector"), sources(true, false)).compilation();

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining("generic I expected 'com.example.await.domain.PendingClarification'");
        assertThat(compilation).hadErrorContaining("but was 'com.example.await.domain.ClarificationRequired'");
    }

    @Test
    void rejectsProjectorWithoutPublicNoArgumentConstructor() throws Exception {
        Compilation compilation = compile("bad-constructor", yaml("PrivateProjector"), sources(false, true)).compilation();

        assertThat(compilation).failed();
        assertThat(compilation).hadErrorContaining("public concrete class with a public no-arg constructor");
    }

    private Fixture compile(String id, String yaml, Map<String, String> sources) throws Exception {
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
            .withOptions("-Apipeline.config=" + config, "-Apipeline.generatedSourcesDir=" + generated,
                "-Apipeline.transport=LOCAL")
            .compile(files);
        return new Fixture(generated, compilation);
    }

    private String yaml(String projector) {
        return """
            version: 3
            appName: deferred-completion
            basePackage: com.example.await
            transport: LOCAL
            contract: { input: Request, output: Result }
            types:
              Request: { fields: [[id, string]] }
              Prepared: { fields: [[id, string]] }
              ClarificationRequired: { fields: [[id, string]] }
              PreparationDecision:
                variants: { prepared: Prepared, clarification: ClarificationRequired }
              PendingClarification: { fields: [[id, string]] }
              ClarificationAnswer: { fields: [[text, string]] }
              Result: { fields: [[id, string]] }
            steps:
              - { name: Prepare, service: com.example.await.PrepareService, cardinality: ONE_TO_ONE, input: Request, output: PreparationDecision }
              - name: Clarify
                service: com.example.await.ClarifyService
                cardinality: ONE_TO_ONE
                input: PreparationDecision
                accepts: [ClarificationRequired]
                output: Prepared
                await:
                  operationOutput:
                    type: PendingClarification
                    java: com.example.await.domain.PendingClarification
                  timeout: PT8H
                  idempotency: { fields: [id] }
                  correlation: { strategy: interactionId }
                  completion:
                    type: ClarificationAnswer
                    projector: com.example.await.%s
                  transport: { type: interaction-api }
              - { name: Finish, service: com.example.await.FinishService, cardinality: ONE_TO_ONE, input: Prepared, output: Result, terminal: true }
            """.formatted(projector);
    }

    private Map<String, String> sources(boolean wrongInput, boolean privateConstructor) {
        String projectorInput = wrongInput ? "ClarificationRequired" : "PendingClarification";
        String constructor = privateConstructor ? "private Projector() { }" : "public Projector() { }";
        String projectorName = privateConstructor ? "PrivateProjector" : wrongInput ? "WrongInputProjector" : "ClarificationProjector";
        return Map.ofEntries(
            Map.entry("com.example.await.domain.Request", "package com.example.await.domain; public record Request(String id) {}"),
            Map.entry("com.example.await.domain.PreparationDecision", "package com.example.await.domain; public sealed interface PreparationDecision permits Prepared, ClarificationRequired { String id(); }"),
            Map.entry("com.example.await.domain.Prepared", "package com.example.await.domain; public record Prepared(String id) implements PreparationDecision {}"),
            Map.entry("com.example.await.domain.ClarificationRequired", "package com.example.await.domain; public record ClarificationRequired(String id) implements PreparationDecision {}"),
            Map.entry("com.example.await.domain.PendingClarification", "package com.example.await.domain; public record PendingClarification(String id) {}"),
            Map.entry("com.example.await.domain.ClarificationAnswer", "package com.example.await.domain; public record ClarificationAnswer(String text) {}"),
            Map.entry("com.example.await.domain.Result", "package com.example.await.domain; public record Result(String id) {}"),
            Map.entry("com.example.await.PrepareService", """
                package com.example.await; import com.example.await.domain.*;
                public class PrepareService implements org.pipelineframework.service.ReactiveService<Request, PreparationDecision> {
                  public io.smallrye.mutiny.Uni<PreparationDecision> process(Request in) { return io.smallrye.mutiny.Uni.createFrom().item(new ClarificationRequired(in.id())); }
                }
                """),
            Map.entry("com.example.await.ClarifyService", """
                package com.example.await; import com.example.await.domain.*;
                public class ClarifyService implements org.pipelineframework.service.ReactiveService<ClarificationRequired, PendingClarification> {
                  public io.smallrye.mutiny.Uni<PendingClarification> process(ClarificationRequired in) { return io.smallrye.mutiny.Uni.createFrom().item(new PendingClarification(in.id())); }
                }
                """),
            Map.entry("com.example.await.FinishService", """
                package com.example.await; import com.example.await.domain.*;
                public class FinishService implements org.pipelineframework.service.ReactiveService<Prepared, Result> {
                  public io.smallrye.mutiny.Uni<Result> process(Prepared in) { return io.smallrye.mutiny.Uni.createFrom().item(new Result(in.id())); }
                }
                """),
            Map.entry("com.example.await." + projectorName, """
                package com.example.await; import com.example.await.domain.*;
                public class %s implements org.pipelineframework.awaitable.AwaitCompletionProjector<%s, ClarificationAnswer, Prepared> {
                  %s
                  public Prepared project(%s request, ClarificationAnswer completion, org.pipelineframework.awaitable.AwaitCompletionMetadata metadata) { return new Prepared(request.id()); }
                }
                """.formatted(projectorName, projectorInput, constructor.replace("Projector", projectorName), projectorInput))
        );
    }

    private record Fixture(Path generatedRoot, Compilation compilation) {
        Path generated(String name) throws Exception {
            try (var paths = Files.walk(generatedRoot)) {
                List<Path> generatedFiles = paths.filter(path -> path.getFileName().toString().endsWith(".java"))
                    .toList();
                return generatedFiles.stream()
                    .filter(path -> path.getFileName().toString().equals(name + ".java"))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Missing " + name + " among " + generatedFiles));
            }
        }

        String metadata(String name) throws Exception {
            return compilation.generatedFile(javax.tools.StandardLocation.CLASS_OUTPUT, "META-INF/pipeline", name)
                .orElseThrow().getCharContent(true).toString();
        }
    }
}
