package org.pipelineframework.processor.phase;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.boundary.PipelineCheckpointConfig;
import org.pipelineframework.config.boundary.PipelineInputBoundaryConfig;
import org.pipelineframework.config.boundary.PipelineOutputBoundaryConfig;
import org.pipelineframework.config.boundary.PipelineSubscriptionConfig;
import org.pipelineframework.config.template.PipelinePlatform;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateStep;

class CheckpointBoundaryValidatorTest {

    private final CheckpointBoundaryValidator validator = new CheckpointBoundaryValidator();

    @TempDir
    Path tempDir;

    @Test
    void validateDoesNothingWhenNoBoundariesAreDeclared() throws IOException {
        PipelineTemplateConfig templateConfig = templateConfig(null, null, PipelinePlatform.COMPUTE);
        assertDoesNotThrow(() -> validator.validate(templateConfig, tempDir, null, null));
    }

    @Test
    void validateFailsForFunctionPlatform() throws IOException {
        writeApplicationProperties("pipeline.orchestrator.mode=QUEUE_ASYNC");
        PipelineTemplateConfig templateConfig = templateConfig(
            new PipelineInputBoundaryConfig(new PipelineSubscriptionConfig("orders-ready", null)),
            null,
            PipelinePlatform.FUNCTION);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> validator.validate(templateConfig, tempDir, null, null));
        assertEquals("Checkpoint publication/subscription is not supported on FUNCTION pipelines",
            exception.getMessage());
    }

    @Test
    void validateFailsWhenQueueAsyncModeIsMissing() {
        PipelineTemplateConfig templateConfig = templateConfig(
            null,
            new PipelineOutputBoundaryConfig(new PipelineCheckpointConfig("orders-dispatched", List.of("orderId"))),
            PipelinePlatform.COMPUTE);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> validator.validate(templateConfig, tempDir, null, null));
        assertEquals("Checkpoint publication/subscription requires pipeline.orchestrator.mode=QUEUE_ASYNC",
            exception.getMessage());
    }

    @Test
    void validateReportsCheckpointPublicationNoteToMessager() throws IOException {
        writeApplicationProperties("pipeline.orchestrator.mode=QUEUE_ASYNC");
        Messager messager = mock(Messager.class);
        PipelineTemplateConfig templateConfig = templateConfig(
            null,
            new PipelineOutputBoundaryConfig(new PipelineCheckpointConfig("orders-dispatched", List.of())),
            PipelinePlatform.COMPUTE);

        assertDoesNotThrow(() -> validator.validate(templateConfig, tempDir, null, messager));
        verify(messager).printMessage(eq(javax.tools.Diagnostic.Kind.NOTE), contains("orders-dispatched"));
    }

    @Test
    void validateFailsWhenSubscriptionMapperTypeCannotBeResolved() throws IOException {
        writeApplicationProperties("pipeline.orchestrator.mode=QUEUE_ASYNC");
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        Elements elements = mock(Elements.class);
        Types types = mock(Types.class);
        when(processingEnv.getElementUtils()).thenReturn(elements);
        when(processingEnv.getTypeUtils()).thenReturn(types);
        when(processingEnv.getSourceVersion()).thenReturn(SourceVersion.RELEASE_21);
        when(elements.getTypeElement("com.example.mapper.MissingMapper")).thenReturn(null);

        PipelineTemplateConfig templateConfig = templateConfig(
            new PipelineInputBoundaryConfig(new PipelineSubscriptionConfig("orders-ready", "com.example.mapper.MissingMapper")),
            null,
            PipelinePlatform.COMPUTE);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> validator.validate(templateConfig, tempDir, processingEnv, null));
        assertEquals("Subscription mapper type not found: com.example.mapper.MissingMapper", exception.getMessage());
    }

    @Test
    void validateDoesNothingWhenTemplateConfigIsNull() {
        assertDoesNotThrow(() -> validator.validate(null, tempDir, null, null));
    }

    @Test
    void validateFailsWhenStepsListIsEmpty() throws IOException {
        writeApplicationProperties("pipeline.orchestrator.mode=QUEUE_ASYNC");
        PipelineTemplateConfig templateConfig = new PipelineTemplateConfig(
            1,
            "checkpoint-test",
            "com.example.pipeline",
            "GRPC",
            PipelinePlatform.COMPUTE,
            Map.of(),
            List.of(), // empty steps
            Map.of(),
            new PipelineInputBoundaryConfig(new PipelineSubscriptionConfig("orders-ready", null)),
            null);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> validator.validate(templateConfig, tempDir, null, null));
        assertEquals("Checkpoint publication/subscription requires at least one pipeline step",
            exception.getMessage());
    }

    @Test
    void validateSucceedsWhenQueueAsyncModeReadFromModuleDir() throws IOException {
        writeApplicationProperties("pipeline.orchestrator.mode=QUEUE_ASYNC");
        Messager messager = mock(Messager.class);
        PipelineTemplateConfig templateConfig = templateConfig(
            null,
            new PipelineOutputBoundaryConfig(new PipelineCheckpointConfig("orders-dispatched", List.of("orderId"))),
            PipelinePlatform.COMPUTE);

        assertDoesNotThrow(() -> validator.validate(templateConfig, tempDir, null, messager));
    }

    @Test
    void validateFailsWhenQueueAsyncModeIsNotSetInModuleDir() throws IOException {
        writeApplicationProperties("pipeline.orchestrator.mode=SYNC");
        PipelineTemplateConfig templateConfig = templateConfig(
            null,
            new PipelineOutputBoundaryConfig(new PipelineCheckpointConfig("orders-dispatched", List.of())),
            PipelinePlatform.COMPUTE);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> validator.validate(templateConfig, tempDir, null, null));
        assertEquals("Checkpoint publication/subscription requires pipeline.orchestrator.mode=QUEUE_ASYNC",
            exception.getMessage());
    }

    @Test
    void validateFailsWhenNoModuleDirAndNoProcessingEnv() {
        PipelineTemplateConfig templateConfig = templateConfig(
            null,
            new PipelineOutputBoundaryConfig(new PipelineCheckpointConfig("orders-dispatched", List.of())),
            PipelinePlatform.COMPUTE);

        // No module dir, no processingEnv -> orchestrator mode is null -> fails
        IllegalStateException exception = assertThrows(IllegalStateException.class,
            () -> validator.validate(templateConfig, null, null, null));
        assertEquals("Checkpoint publication/subscription requires pipeline.orchestrator.mode=QUEUE_ASYNC",
            exception.getMessage());
    }

    @Test
    void validateSubscriptionWithNullMapperSkipsMapperValidation() throws IOException {
        writeApplicationProperties("pipeline.orchestrator.mode=QUEUE_ASYNC");
        PipelineTemplateConfig templateConfig = templateConfig(
            new PipelineInputBoundaryConfig(new PipelineSubscriptionConfig("orders-ready", null)),
            null,
            PipelinePlatform.COMPUTE);

        // null mapper should not attempt type resolution
        assertDoesNotThrow(() -> validator.validate(templateConfig, tempDir, null, null));
    }

    @Test
    void validateSubscriptionWithBlankMapperSkipsMapperValidation() throws IOException {
        writeApplicationProperties("pipeline.orchestrator.mode=QUEUE_ASYNC");
        PipelineTemplateConfig templateConfig = templateConfig(
            new PipelineInputBoundaryConfig(new PipelineSubscriptionConfig("orders-ready", "  ")),
            null,
            PipelinePlatform.COMPUTE);

        // blank mapper should not attempt type resolution
        assertDoesNotThrow(() -> validator.validate(templateConfig, tempDir, null, null));
    }

    private PipelineTemplateConfig templateConfig(
        PipelineInputBoundaryConfig input,
        PipelineOutputBoundaryConfig output,
        PipelinePlatform platform
    ) {
        return new PipelineTemplateConfig(
            1,
            "checkpoint-test",
            "com.example.pipeline",
            "GRPC",
            platform,
            Map.of(),
            List.of(new PipelineTemplateStep("Order Ready", "ONE_TO_ONE", "com.example.domain.OrderRequest", null,
                "com.example.domain.ReadyOrder", null, null)),
            Map.of(),
            input,
            output);
    }

    private void writeApplicationProperties(String content) throws IOException {
        Path resourcesDir = tempDir.resolve("src/main/resources");
        Files.createDirectories(resourcesDir);
        Files.writeString(resourcesDir.resolve("application.properties"), content);
    }
}