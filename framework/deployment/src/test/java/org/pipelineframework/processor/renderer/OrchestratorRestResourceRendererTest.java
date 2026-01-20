package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.processing.ProcessingEnvironment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.*;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrchestratorRestResourceRendererTest {

    @TempDir
    Path tempDir;

    @Test
    void rendersUnaryRestResource() throws IOException {
        OrchestratorBinding binding = buildBinding(false, false);
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));

        OrchestratorRestResourceRenderer renderer = new OrchestratorRestResourceRenderer();
        renderer.render(binding, new GenerationContext(processingEnv, tempDir, DeploymentRole.REST_SERVER,
            java.util.Set.of(), null, null));

        Path generatedSource = tempDir.resolve("com/example/orchestrator/service/PipelineRunResource.java");
        String source = Files.readString(generatedSource);

        assertTrue(source.contains("package com.example.orchestrator.service;"));
        assertTrue(source.contains("@Path(\"/pipeline\")"));
        assertTrue(source.contains("@ApplicationScoped"));
        assertTrue(source.contains("@Path(\"/run\")"));
        assertTrue(source.contains("public Uni<OutputTypeDto> run(InputTypeDto input)"));
        assertTrue(source.contains("return pipelineExecutionService.executePipelineUnary("));
    }

    @Test
    void rendersStreamingRestResource() throws IOException {
        OrchestratorBinding binding = buildBinding(true, true);
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new TestFiler(tempDir));

        OrchestratorRestResourceRenderer renderer = new OrchestratorRestResourceRenderer();
        renderer.render(binding, new GenerationContext(processingEnv, tempDir, DeploymentRole.REST_SERVER,
            java.util.Set.of(), null, null));

        Path generatedSource = tempDir.resolve("com/example/orchestrator/service/PipelineRunResource.java");
        String source = Files.readString(generatedSource);

        assertTrue(source.contains("@Consumes(\"application/x-ndjson\")"));
        assertTrue(source.contains("@Produces(\"application/x-ndjson\")"));
        assertTrue(source.contains("@RestStreamElementType(\"application/json\")"));
        assertTrue(source.contains("public Multi<OutputTypeDto> run(Multi<InputTypeDto> input)"));
        assertTrue(source.contains("return pipelineExecutionService.executePipelineStreaming(input);"));
    }

    private OrchestratorBinding buildBinding(boolean inputStreaming, boolean outputStreaming) {
        PipelineStepModel model = new PipelineStepModel(
            "OrchestratorService",
            "OrchestratorService",
            "com.example.orchestrator.service",
            com.squareup.javapoet.ClassName.get("com.example.orchestrator.service", "OrchestratorService"),
            null,
            null,
            streamingShape(inputStreaming, outputStreaming),
            java.util.Set.of(GenerationTarget.GRPC_SERVICE),
            ExecutionMode.DEFAULT,
            DeploymentRole.ORCHESTRATOR_CLIENT,
            false,
            null
        );

        return new OrchestratorBinding(
            model,
            "com.example",
            "REST",
            "InputType",
            "OutputType",
            inputStreaming,
            outputStreaming,
            "ProcessAlphaService",
            StreamingShape.UNARY_UNARY,
            null,
            null,
            null
        );
    }

    private StreamingShape streamingShape(boolean inputStreaming, boolean outputStreaming) {
        if (inputStreaming && outputStreaming) {
            return StreamingShape.STREAMING_STREAMING;
        }
        if (inputStreaming) {
            return StreamingShape.STREAMING_UNARY;
        }
        if (outputStreaming) {
            return StreamingShape.UNARY_STREAMING;
        }
        return StreamingShape.UNARY_UNARY;
    }
}
