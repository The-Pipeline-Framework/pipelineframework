package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.processing.ProcessingEnvironment;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.RestBinding;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RestFunctionHandlerRendererTest {

    @TempDir
    Path tempDir;

    @Test
    void rendersUnaryFunctionHandler() throws IOException {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));

        RestFunctionHandlerRenderer renderer = new RestFunctionHandlerRenderer();
        renderer.render(new RestBinding(unaryModel(), null),
            new GenerationContext(processingEnv, tempDir, DeploymentRole.REST_SERVER,
                java.util.Set.of(), null, null));

        Path generatedSource =
            tempDir.resolve("org/example/search/parse/service/pipeline/ParsedDocumentFunctionHandler.java");
        String source = Files.readString(generatedSource);

        assertTrue(source.contains("implements RequestHandler<ParsedDocumentDto, IndexAckDto>"));
        assertTrue(source.contains("@Named(\"ParsedDocumentFunctionHandler\")"));
        assertTrue(source.contains("ParsedDocumentResource resource"));
        assertTrue(source.contains("handleRequest(ParsedDocumentDto input, Context context)"));
        assertTrue(source.contains("return resource.process(input).await().indefinitely()"));
    }

    @Test
    void rejectsStreamingShape() {
        RestFunctionHandlerRenderer renderer = new RestFunctionHandlerRenderer();
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));

        assertThrows(IllegalStateException.class, () ->
            renderer.render(new RestBinding(streamingModel(), null),
                new GenerationContext(processingEnv, tempDir, DeploymentRole.REST_SERVER,
                    java.util.Set.of(), null, null)));
    }

    private PipelineStepModel unaryModel() {
        return new PipelineStepModel.Builder()
            .serviceName("ProcessParsedDocumentService")
            .generatedName("ParsedDocumentService")
            .servicePackage("org.example.search.parse.service")
            .serviceClassName(ClassName.get("org.example.search.parse.service", "ProcessParsedDocumentService"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .enabledTargets(java.util.Set.of(GenerationTarget.REST_RESOURCE))
            .inputMapping(new TypeMapping(
                ClassName.get("org.example.search.common.domain", "ParsedDocument"),
                ClassName.get("org.example.search.common.mapper", "ParsedDocumentMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.example.search.common.domain", "IndexAck"),
                ClassName.get("org.example.search.common.mapper", "IndexAckMapper"),
                true))
            .build();
    }

    private PipelineStepModel streamingModel() {
        return new PipelineStepModel.Builder()
            .serviceName("ProcessParsedDocumentService")
            .generatedName("ParsedDocumentService")
            .servicePackage("org.example.search.parse.service")
            .serviceClassName(ClassName.get("org.example.search.parse.service", "ProcessParsedDocumentService"))
            .streamingShape(StreamingShape.UNARY_STREAMING)
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .enabledTargets(java.util.Set.of(GenerationTarget.REST_RESOURCE))
            .inputMapping(new TypeMapping(
                ClassName.get("org.example.search.common.domain", "ParsedDocument"),
                ClassName.get("org.example.search.common.mapper", "ParsedDocumentMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.example.search.common.domain", "IndexAck"),
                ClassName.get("org.example.search.common.mapper", "IndexAckMapper"),
                true))
            .build();
    }
}
