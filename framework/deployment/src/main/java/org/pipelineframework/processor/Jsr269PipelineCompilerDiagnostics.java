package org.pipelineframework.processor;

import java.util.Objects;
import javax.annotation.processing.Messager;
import javax.tools.Diagnostic;

/** Adapts host-neutral compiler diagnostics to the production JSR-269 messager. */
final class Jsr269PipelineCompilerDiagnostics implements PipelineCompilerDiagnostics {

    private final Messager messager;

    Jsr269PipelineCompilerDiagnostics(Messager messager) {
        this.messager = Objects.requireNonNull(messager, "messager must not be null");
    }

    @Override
    public void report(Severity severity, String message) {
        Diagnostic.Kind kind = switch (Objects.requireNonNull(severity, "severity must not be null")) {
            case ERROR -> Diagnostic.Kind.ERROR;
            case WARNING -> Diagnostic.Kind.WARNING;
            case NOTE -> Diagnostic.Kind.NOTE;
        };
        messager.printMessage(kind, Objects.requireNonNull(message, "message must not be null"));
    }
}
