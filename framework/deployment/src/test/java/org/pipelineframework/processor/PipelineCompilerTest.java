package org.pipelineframework.processor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doReturn;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;

import org.junit.jupiter.api.Test;
import org.pipelineframework.annotation.PipelineOrchestrator;
import org.pipelineframework.annotation.PipelinePlugin;
import org.pipelineframework.annotation.PipelineStep;

class PipelineCompilerTest {

    private static Set<Element> noElements() {
        return Collections.emptySet();
    }

    @Test
    void executesYamlDrivenCompilationOnlyOnceAcrossRounds() throws Exception {
        PipelineCompilationPhase phase = mock(PipelineCompilationPhase.class);
        PipelineCompiler compiler = new PipelineCompiler(List.of(phase));

        Path pipelineConfig = Files.createTempFile("pipeline-compiler-test", ".yaml");
        Files.writeString(pipelineConfig, "appName: test\nbasePackage: com.example\nsteps: []\n");

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getOptions()).thenReturn(Map.of("pipeline.config", pipelineConfig.toString()));
        when(processingEnv.getMessager()).thenReturn(mock(Messager.class));
        compiler.init(processingEnv);

        RoundEnvironment roundOne = mock(RoundEnvironment.class);
        doReturn(noElements()).when(roundOne).getElementsAnnotatedWith(PipelineStep.class);
        doReturn(noElements()).when(roundOne).getElementsAnnotatedWith(PipelineOrchestrator.class);
        doReturn(noElements()).when(roundOne).getElementsAnnotatedWith(PipelinePlugin.class);
        when(roundOne.processingOver()).thenReturn(false);

        RoundEnvironment roundTwo = mock(RoundEnvironment.class);
        doReturn(noElements()).when(roundTwo).getElementsAnnotatedWith(PipelineStep.class);
        doReturn(noElements()).when(roundTwo).getElementsAnnotatedWith(PipelineOrchestrator.class);
        doReturn(noElements()).when(roundTwo).getElementsAnnotatedWith(PipelinePlugin.class);
        when(roundTwo.processingOver()).thenReturn(false);

        boolean firstResult = compiler.process(Set.<TypeElement>of(), roundOne);
        boolean secondResult = compiler.process(Set.<TypeElement>of(), roundTwo);

        assertTrue(firstResult, "First YAML-driven round should execute compilation");
        assertFalse(secondResult, "Subsequent rounds should not execute compilation again");
        verify(phase, times(1)).execute(org.mockito.ArgumentMatchers.any(PipelineCompilationContext.class));
    }
}
