package org.pipelineframework.processor.renderer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;

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
        assertTrue(source.contains("CommandStepSupport support"));
        assertTrue(source.contains("CommandStepDescriptorFactory descriptorFactory"));
        assertTrue(source.contains("SearchIndexDocumentCommandIdGenerator commandIdGenerator"));
        assertTrue(source.contains("return support.execute(descriptorFactory.descriptor(\"ProcessWriteSearchIndexDocumentService\", "
            + "null, \"com.example.search.SearchIndexDocument\", \"com.example.search.SearchIndexWriteResult\", "
            + "\"com.example.search.SearchIndexDocumentCommandIdGenerator\"), commandIdGenerator, input)"));
    }

    @Test
    void rendersRestTransportDtoTypes() throws IOException {
        new CommandClientStepRenderer().render(commandStepModel(), generationContext("REST"));

        String source = generatedSource();

        assertTrue(source.contains("import com.example.search.common.dto.SearchIndexDocumentDto;"));
        assertTrue(source.contains("import com.example.search.common.dto.SearchIndexWriteResultDto;"));
        assertTrue(source.contains("implements StepOneToOne<SearchIndexDocumentDto, SearchIndexWriteResultDto>"));
    }

    @Test
    void rendersGrpcTransportPipelineTypes() throws IOException {
        new CommandClientStepRenderer().render(commandStepModel(), generationContext("GRPC"));

        String source = generatedSource();

        assertTrue(source.contains("import com.example.search.grpc.PipelineTypes;"));
        assertTrue(source.contains("implements StepOneToOne<PipelineTypes.SearchIndexDocument, PipelineTypes.SearchIndexWriteResult>"));
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
