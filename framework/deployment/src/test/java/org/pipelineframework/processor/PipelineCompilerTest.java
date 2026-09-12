package org.pipelineframework.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class PipelineCompilerTest {

    @Test
    void executesAnImmutablePhaseOrderOncePerCompileInvocation() {
        List<String> executionOrder = new ArrayList<>();
        List<PipelineCompilationPhase> phases = new ArrayList<>(List.of(
            new RecordingPhase("discover", executionOrder),
            new RecordingPhase("analyze", executionOrder),
            new RecordingPhase("generate", executionOrder)));
        PipelineCompiler compiler = new PipelineCompiler(phases);
        phases.add(new RecordingPhase("late", executionOrder));
        PipelineCompilationContext context = new PipelineCompilationContext(null, org.pipelineframework.processor.Jsr269SourceInventory.empty());

        compiler.compile(context);
        compiler.compile(context);

        assertEquals(List.of("discover", "analyze", "generate", "discover", "analyze", "generate"),
            executionOrder);
    }

    @Test
    void identifiesTheFailedPhaseAndPreservesItsCause() throws Exception {
        Exception phaseFailure = new IllegalStateException("invalid model", new IllegalArgumentException("bad type"));
        PipelineCompilationPhase failing = mock(PipelineCompilationPhase.class);
        PipelineCompilationPhase skipped = mock(PipelineCompilationPhase.class);
        org.mockito.Mockito.when(failing.name()).thenReturn("semantic-analysis");
        org.mockito.Mockito.doThrow(phaseFailure).when(failing).execute(org.mockito.ArgumentMatchers.any());
        PipelineCompiler compiler = new PipelineCompiler(List.of(failing, skipped));

        PipelineCompilationException thrown = assertThrows(PipelineCompilationException.class,
            () -> compiler.compile(new PipelineCompilationContext(null, org.pipelineframework.processor.Jsr269SourceInventory.empty())));

        assertEquals("semantic-analysis", thrown.phaseName());
        assertSame(phaseFailure, thrown.getCause());
        verify(skipped, never()).execute(org.mockito.ArgumentMatchers.any());
    }

    private record RecordingPhase(String name, List<String> executionOrder) implements PipelineCompilationPhase {
        @Override
        public void execute(PipelineCompilationContext context) {
            executionOrder.add(name);
        }
    }
}
