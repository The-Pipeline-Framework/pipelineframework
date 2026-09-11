package org.pipelineframework.processor.awaitable;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;
import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pipelineframework.config.template.*;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.DeferredCompletionDefinition;
import org.pipelineframework.processor.ir.StepDefinition;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CommandCallbackTypeBindingTest {
    @TempDir Path output;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void projectorUsesInferredCanonicalInputInsteadOfAcknowledgement(boolean inputProjector) throws Exception {
        var definitions = new java.util.LinkedHashMap<String, PipelineTemplateTypeDefinition>();
        var sources = new java.util.ArrayList<JavaFileObject>();
        for (String name : List.of("Input", "Accepted", "Completion", "Result")) {
            definitions.put(name, new PipelineTemplateTypeDefinition.RecordType(name, List.of()));
            sources.add(source(name, "public record " + name + "() {}"));
        }
        sources.add(source("Endpoint", """
            public class Endpoint implements org.pipelineframework.connector.ProviderCallbackEndpointResolver {
                public java.net.URI resolve(org.pipelineframework.connector.ProviderCallbackRequest request) {
                    return java.net.URI.create("https://app.example/callback");
                }
            }
            """));
        sources.add(source("Authenticator", """
            public class Authenticator implements org.pipelineframework.connector.ProviderCallbackAuthenticator {
                public java.util.concurrent.CompletionStage<java.util.Optional<org.pipelineframework.connector.ProviderCallbackActor>> authenticate(
                    org.pipelineframework.connector.ProviderCallbackAuthenticationRequest request) {
                    return java.util.concurrent.CompletableFuture.completedFuture(java.util.Optional.of(
                        new org.pipelineframework.connector.ProviderCallbackActor("provider")));
                }
            }
            """));
        String contextType = inputProjector ? "Input" : "Accepted";
        sources.add(source("Projector", "public class Projector implements org.pipelineframework.awaitable.AwaitCompletionProjector<"
            + contextType + ", Completion, Result> { public Result project(" + contextType
            + " input, Completion completion, org.pipelineframework.awaitable.AwaitCompletionMetadata metadata) { return new Result(); } }"));

        var authored = mock(PipelineTemplateStep.class);
        when(authored.name()).thenReturn("StartJob");
        when(authored.inputTypeName()).thenReturn("Input");
        when(authored.outputTypeName()).thenReturn("Result");
        var config = mock(PipelineTemplateConfig.class);
        when(config.dialect()).thenReturn(PipelineTemplateDialect.V3);
        when(config.basePackage()).thenReturn("com.example");
        when(config.typeModel()).thenReturn(new PipelineTemplateTypeModel(definitions));
        when(config.steps()).thenReturn(List.of(authored));
        var step = mock(StepDefinition.class);
        when(step.name()).thenReturn("StartJob");
        when(step.deferredCompletion()).thenReturn(Optional.of(new DeferredCompletionDefinition("Accepted", Optional.empty(),
            "PT1M", List.of(), "signedResumeToken", new DeferredCompletionDefinition.ConnectorCallbackDefinition("completed",
                ClassName.get("com.example.domain", "Endpoint"), ClassName.get("com.example.domain", "Authenticator")),
            Optional.of(new DeferredCompletionDefinition.CompletionProjectionDefinition("Completion",
                ClassName.get("com.example.domain", "Projector"))))));
        var invoked = new AtomicBoolean();
        var bound = new AtomicBoolean();
        var processor = new AbstractProcessor() {
            @Override public Set<String> getSupportedAnnotationTypes() { return Set.of("*"); }
            @Override public SourceVersion getSupportedSourceVersion() { return SourceVersion.latestSupported(); }
            @Override public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment round) {
                if (!round.processingOver() && invoked.compareAndSet(false, true)) {
                    var context = new PipelineCompilationContext(processingEnv, round);
                    context.setPipelineTemplateConfig(config);
                    bound.set(new AwaitStepTypeBindingResolver().resolve(context, step).isPresent());
                }
                return false;
            }
        };
        var compiler = ToolProvider.getSystemJavaCompiler();
        var diagnostics = new javax.tools.DiagnosticCollector<JavaFileObject>();
        try (var manager = compiler.getStandardFileManager(diagnostics, java.util.Locale.ROOT, java.nio.charset.StandardCharsets.UTF_8)) {
            var task = compiler.getTask(new java.io.StringWriter(), manager, diagnostics,
                List.of("-proc:only", "-classpath", System.getProperty("java.class.path"), "-d", output.toString()),
                List.of(), sources);
            task.setProcessors(List.of(processor));
            assertEquals(inputProjector, task.call(), diagnostics.getDiagnostics().toString());
            assertTrue(invoked.get());
            assertEquals(inputProjector, bound.get());
        }
    }

    private JavaFileObject source(String name, String body) {
        return new SimpleJavaFileObject(URI.create("string:///com/example/domain/" + name + ".java"), JavaFileObject.Kind.SOURCE) {
            @Override public CharSequence getCharContent(boolean ignoreEncodingErrors) {
                return "package com.example.domain;\n" + body;
            }
        };
    }
}
