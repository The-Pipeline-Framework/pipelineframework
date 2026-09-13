package org.pipelineframework.processor;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import javax.annotation.processing.Messager;
import javax.tools.Diagnostic;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class Jsr269PipelineCompilerDiagnosticsTest {

    @Test
    void forwardsEveryCompilerSeverityToTheJsr269MessagerInOrder() {
        Messager messager = mock(Messager.class);
        PipelineCompilerDiagnostics diagnostics = new Jsr269PipelineCompilerDiagnostics(messager);

        diagnostics.error("error");
        diagnostics.warning("warning");
        diagnostics.note("note");

        InOrder ordered = inOrder(messager);
        ordered.verify(messager).printMessage(Diagnostic.Kind.ERROR, "error");
        ordered.verify(messager).printMessage(Diagnostic.Kind.WARNING, "warning");
        ordered.verify(messager).printMessage(Diagnostic.Kind.NOTE, "note");
    }
}
