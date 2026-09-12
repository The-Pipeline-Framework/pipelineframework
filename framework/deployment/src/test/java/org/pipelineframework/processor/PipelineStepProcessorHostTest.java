package org.pipelineframework.processor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.annotation.PipelinePlugin;
import org.pipelineframework.annotation.PipelineStep;

class PipelineStepProcessorHostTest {

    @TempDir
    Path tempDir;

    @Test
    void executesYamlDrivenCompilationOnlyOnceAcrossRoundsAndCreatesTheHostContext() throws Exception {
        PipelineCompilationPhase phase = mock(PipelineCompilationPhase.class);
        PipelineStepProcessor processor = new PipelineStepProcessor(new PipelineCompiler(java.util.List.of(phase)));
        ProcessingEnvironment processingEnv = processingEnvironmentWithYaml();
        RoundEnvironment firstRound = emptyRound();
        RoundEnvironment secondRound = emptyRound();
        processor.init(processingEnv);

        boolean firstResult = processor.process(Set.<TypeElement>of(), firstRound);
        boolean secondResult = processor.process(Set.<TypeElement>of(), secondRound);

        assertFalse(firstResult);
        assertFalse(secondResult);
        ArgumentCaptor<PipelineCompilationContext> context = ArgumentCaptor.forClass(PipelineCompilationContext.class);
        verify(phase, times(1)).execute(context.capture());
        assertSame(processingEnv, context.getValue().getProcessingEnv());
        assertSame(firstRound, context.getValue().getRoundEnv());
        assertSame(processor.getClass().getClassLoader(), context.getValue().getRepresentationProviderClassLoader());
    }

    @Test
    void emitsTheExistingPhaseFailureDiagnosticsFromTheHost() throws Exception {
        PipelineCompilationPhase phase = mock(PipelineCompilationPhase.class);
        when(phase.name()).thenReturn("semantic-analysis");
        org.mockito.Mockito.doThrow(new IllegalStateException("invalid model", new IOException("bad type")))
            .when(phase).execute(org.mockito.ArgumentMatchers.any());
        PipelineStepProcessor processor = new PipelineStepProcessor(new PipelineCompiler(java.util.List.of(phase)));
        ProcessingEnvironment processingEnv = processingEnvironmentWithYaml();
        Messager messager = processingEnv.getMessager();
        processor.init(processingEnv);

        boolean result = processor.process(Set.<TypeElement>of(), emptyRound());

        assertFalse(result);
        verify(messager).printMessage(eq(Diagnostic.Kind.ERROR),
            eq("Pipeline compilation failed in phase 'semantic-analysis': invalid model"));
        verify(messager).printMessage(eq(Diagnostic.Kind.NOTE), eq("Cause: IOException: bad type"));
    }

    private ProcessingEnvironment processingEnvironmentWithYaml() throws IOException {
        Path pipelineConfig = tempDir.resolve("pipeline.yaml");
        Files.writeString(pipelineConfig, "appName: test\nbasePackage: com.example\nsteps: []\n");
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(Map.of("pipeline.config", pipelineConfig.toString()));
        when(processingEnv.getMessager()).thenReturn(mock(Messager.class));
        return processingEnv;
    }

    private static RoundEnvironment emptyRound() {
        RoundEnvironment round = mock(RoundEnvironment.class);
        doReturn(Set.<Element>of()).when(round).getElementsAnnotatedWith(PipelineStep.class);
        doReturn(Set.<Element>of()).when(round).getElementsAnnotatedWith(PipelineOrchestrator.class);
        doReturn(Set.<Element>of()).when(round).getElementsAnnotatedWith(PipelinePlugin.class);
        return round;
    }
}
