package org.pipelineframework.processor.renderer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ConnectorOperationSelection;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.command.CommandDuplicatePolicy;
import org.pipelineframework.connector.CommandExecutionPosture;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandPolicy;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;

class CommandClientStepRendererTest {
    @TempDir
    Path tempDir;

    @Test
    void targetReturnsCommandClientStepTarget() {
        assertEquals(GenerationTarget.COMMAND_CLIENT_STEP, new CommandClientStepRenderer().target());
    }

    @Test
    void rendersOneToOneStepThatDelegatesToCommandSupport() throws IOException {
        PipelineStepModel model = commandStepModel();

        new CommandClientStepRenderer().render(model, generationContext("LOCAL"));

        String source = generatedSource();

        assertTrue(source.contains("implements StepOneToOne<SearchIndexDocument, SearchIndexWriteResult>"));
        assertTrue(source.contains("CommandStep"));
        assertTrue(source.contains("CacheKeyTarget"));
        assertTrue(source.contains("CommandStepSupport support"));
        assertTrue(source.contains("CommandStepDescriptorFactory descriptorFactory"));
        assertTrue(source.contains("SearchIndexDocumentCommandIdGenerator commandIdGenerator"));
        assertTrue(source.contains("return support.execute(descriptorFactory.descriptor(\"ProcessWriteSearchIndexDocumentService\", "
            + "null, \"com.example.search.SearchIndexDocument\", \"com.example.search.SearchIndexWriteResult\", "
            + "\"com.example.search.SearchIndexDocumentCommandIdGenerator\"), commandIdGenerator, input)"));
    }

    @Test
    void rendersOperationFirstCommandFromTypedSelectionWithoutReloadingApplicationYaml() throws IOException {
        ConnectorOperationSelection selection = ConnectorOperationSelection.command(
            "Execute mutation",
            ConnectorBindingName.of("primary-graphql"),
            new ConnectorOperationIdentity(
                ConnectorProviderId.of("graphql.smallrye"), "execute.mutation",
                ConnectorOperationKind.COMMAND, 1),
            1,
            Map.of("catalogue", "application-owned"),
            new ConnectorOperationSelection.CommandSelection(
                ClassName.get("com.example.search", "SearchIndexDocumentCommandIdGenerator"),
                CommandDuplicatePolicy.RETURN_RECORDED,
                new CommandPolicy(false, false, false,
                    Optional.of(CommandExecutionPosture.AUTOMATED),
                    Optional.of(CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED), false)));
        PipelineStepModel model = commandStepModel().toBuilder()
            .connectorOperationSelection(selection).build();

        new CommandClientStepRenderer().render(model, generationContext("LOCAL"));

        String source = generatedSource();
        assertTrue(source.contains("CommandDescriptor.nativeCommand"));
        assertTrue(source.contains("ConnectorBindingName.of(\"primary-graphql\")"));
        assertTrue(source.contains("ConnectorProviderId.of(\"graphql.smallrye\")"));
        assertTrue(source.contains("\"execute.mutation\""));
        assertTrue(source.contains("support.execute(CommandDescriptor.nativeCommand("));
        assertTrue(!source.contains("CommandStepDescriptorFactory"));
    }

    @Test
    void rendersRestTransportDtoTypes() throws IOException {
        new CommandClientStepRenderer().render(commandStepModel(), generationContext("REST"));

        String source = generatedSource();

        assertTrue(source.contains("import com.example.search.common.dto.SearchIndexDocumentDto;"));
        assertTrue(source.contains("import com.example.search.common.dto.SearchIndexWriteResultDto;"));
        assertTrue(source.contains("import com.example.search.common.mapper.SearchIndexDocumentMapper;"));
        assertTrue(source.contains("import com.example.search.common.mapper.SearchIndexWriteResultMapper;"));
        assertTrue(source.contains("implements StepOneToOne<SearchIndexDocumentDto, SearchIndexWriteResultDto>"));
        assertTrue(source.contains("SearchIndexDocumentMapper inputMapper"));
        assertTrue(source.contains("SearchIndexWriteResultMapper outputMapper"));
        assertTrue(source.contains("SearchIndexDocument commandInput = inputMapper.fromExternal(input)"));
        assertTrue(source.contains("\"com.example.search.SearchIndexDocument\""));
        assertTrue(source.contains("\"com.example.search.SearchIndexWriteResult\""));
        assertTrue(source.contains("support.<SearchIndexDocument, SearchIndexWriteResult>execute"));
        assertTrue(source.contains("commandIdGenerator, commandInput)"));
        assertTrue(source.contains(".map(commandOutput -> outputMapper.toExternal(commandOutput))"));
    }

    @Test
    void rendersGrpcTransportPipelineTypes() throws IOException {
        new CommandClientStepRenderer().render(commandStepModel(), generationContext("GRPC"));

        String source = generatedSource();

        assertTrue(source.contains("import com.example.search.grpc.PipelineTypes;"));
        assertTrue(source.contains("import com.example.search.common.mapper.SearchIndexDocumentMapper;"));
        assertTrue(source.contains("import com.example.search.common.mapper.SearchIndexWriteResultMapper;"));
        assertTrue(source.contains("implements StepOneToOne<PipelineTypes.SearchIndexDocument, PipelineTypes.SearchIndexWriteResult>"));
        assertTrue(source.contains("SearchIndexDocument commandInput = inputMapper.fromGrpcFromDto(input)"));
        assertTrue(source.contains("support.<SearchIndexDocument, SearchIndexWriteResult>execute"));
        assertTrue(source.contains(".map(commandOutput -> outputMapper.toDtoToGrpc(commandOutput))"));
    }

    private PipelineStepModel commandStepModel() {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ProcessWriteSearchIndexDocumentService")
            .generatedName("ProcessWriteSearchIndexDocumentService")
            .servicePackage("com.example.search")
            .serviceClassName(ClassName.get("org.pipelineframework.command", "CommandStepDescriptor"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .inputMapping(new TypeMapping(ClassName.get("com.example.search", "SearchIndexDocument"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.search", "SearchIndexWriteResult"), null, false))
            .cacheKeyGenerator(ClassName.get("com.example.search", "SearchIndexDocumentCommandIdGenerator"))
            .enabledTargets(Set.of(GenerationTarget.COMMAND_CLIENT_STEP))
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT)
            .build();
        return model;
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"LOCAL", "REST", "GRPC"})
    void callbackCompletionUsesOneCommandAdapterAndTransportsOnlyFinalOutput(String transport) throws IOException {
        var operation = ConnectorOperationSelection.command("Start", ConnectorBindingName.of("jobs"),
            new ConnectorOperationIdentity(ConnectorProviderId.of("test.jobs"), "start", ConnectorOperationKind.COMMAND, 1),
            1, Map.of(), new ConnectorOperationSelection.CommandSelection(
                ClassName.get("com.example.search", "SearchIndexDocumentCommandIdGenerator"),
                CommandDuplicatePolicy.RETURN_RECORDED, CommandPolicy.none()));
        var callback = new org.pipelineframework.processor.ir.DeferredCompletionSelection.ResolvedConnectorCallback(
            new org.pipelineframework.connector.ConnectorOperationCallbackDescriptor("completed",
                new org.pipelineframework.connector.ConnectorOperationTypeContract("JobCompletion", Optional.empty()), true),
            operation, ClassName.get("com.example", "EndpointResolver"), ClassName.get("com.example", "Authenticator"));
        var completion = new org.pipelineframework.processor.ir.DeferredCompletionSelection(
            ClassName.get("com.example.search", "FinalResult"), "FinalResult", Optional.of("JobCompletion"),
            java.time.Duration.ofMinutes(1), java.util.List.of(), "signedResumeToken", "", Map.of(),
            Optional.of(ClassName.get("com.example", "JobCompletion")), Optional.of(ClassName.get("com.example", "Projector")),
            Optional.of(callback));
        var model = commandStepModel().toBuilder().connectorOperationSelection(operation)
            .deferredCompletionSelection(completion).build();
        new CommandClientStepRenderer().render(model, generationContext(transport));
        String source = generatedSource();
        assertTrue(source.contains("CommandDeferredCompletionSupport deferredCompletion"));
        assertTrue(source.contains("FinalResult"));
        assertTrue(source.contains("callback -> support.<SearchIndexDocument, SearchIndexWriteResult>execute"));
        assertTrue(source.contains("commandIdGenerator, commandInput, callback"));
        assertTrue(source.contains("new AwaitCompletionDescriptor"));
        assertTrue(source.contains("new ConnectorCallbackSelection"));
        assertTrue(!source.contains("SearchIndexWriteResultMapper"));
        assertTrue(!source.contains("AwaitCompletionSupport"));
        assertTrue(!source.contains("DeferredCompletionStep"));
        if (!"LOCAL".equals(transport)) {
            assertTrue(source.contains("FinalResultMapper outputMapper"));
        } else {
            compileCallbackAdapter();
        }
    }

    private void compileCallbackAdapter() throws IOException {
        Map<String, String> fixtures = Map.of(
            "com.example.search.SearchIndexDocument", "public record SearchIndexDocument() {}",
            "com.example.search.SearchIndexWriteResult", "public record SearchIndexWriteResult() {}",
            "com.example.search.FinalResult", "public record FinalResult() {}",
            "com.example.JobCompletion", "public record JobCompletion() {}",
            "com.example.search.SearchIndexDocumentCommandIdGenerator", """
                public class SearchIndexDocumentCommandIdGenerator implements org.pipelineframework.command.CommandIdGenerator<SearchIndexDocument> {
                    public String commandId(org.pipelineframework.command.CommandDescriptor descriptor, SearchIndexDocument input) { return "job"; }
                }
                """,
            "com.example.EndpointResolver", """
                public class EndpointResolver implements org.pipelineframework.awaitable.ProviderCallbackEndpointResolver {
                    public java.net.URI resolve(org.pipelineframework.awaitable.AwaitInteractionRecord interaction, String token) {
                        return java.net.URI.create("https://app.example/callback");
                    }
                }
                """,
            "com.example.Authenticator", "public class Authenticator {}",
            "com.example.Projector", """
                public class Projector implements org.pipelineframework.awaitable.AwaitCompletionProjector<com.example.search.SearchIndexDocument, JobCompletion, com.example.search.FinalResult> {
                    public com.example.search.FinalResult project(com.example.search.SearchIndexDocument input, JobCompletion completion,
                        org.pipelineframework.awaitable.AwaitCompletionMetadata metadata) { return new com.example.search.FinalResult(); }
                }
                """);
        var sources = new java.util.ArrayList<java.io.File>();
        for (var fixture : fixtures.entrySet()) {
            var path = tempDir.resolve(fixture.getKey().replace('.', '/') + ".java");
            Files.createDirectories(path.getParent());
            Files.writeString(path, "package " + fixture.getKey().substring(0, fixture.getKey().lastIndexOf('.'))
                + ";\n" + fixture.getValue());
            sources.add(path.toFile());
        }
        sources.add(tempDir.resolve("com/example/search/pipeline/ProcessWriteSearchIndexDocumentCommandClientStep.java").toFile());
        var compiler = javax.tools.ToolProvider.getSystemJavaCompiler();
        var diagnostics = new javax.tools.DiagnosticCollector<javax.tools.JavaFileObject>();
        try (var manager = compiler.getStandardFileManager(diagnostics, java.util.Locale.ROOT, java.nio.charset.StandardCharsets.UTF_8)) {
            var options = java.util.List.of("-proc:none", "-classpath", System.getProperty("java.class.path"),
                "-d", tempDir.toString());
            assertTrue(compiler.getTask(new java.io.StringWriter(), manager, diagnostics, options,
                java.util.List.of(), manager.getJavaFileObjectsFromFiles(sources)).call(), diagnostics.getDiagnostics().toString());
        }
    }

    private String generatedSource() throws IOException {
        return Files.readString(tempDir.resolve(
            "com/example/search/pipeline/ProcessWriteSearchIndexDocumentCommandClientStep.java"));
    }

    private GenerationContext generationContext(String transport) {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(Map.of("pipeline.transport", transport));
        return new GenerationContext(
            processingEnv,
            tempDir,
            DeploymentRole.ORCHESTRATOR_CLIENT,
            Set.of(),
            null,
            null);
    }
}
