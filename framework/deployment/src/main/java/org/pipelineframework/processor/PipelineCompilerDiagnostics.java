package org.pipelineframework.processor;

/** Host-neutral compiler diagnostic capability for messages without source anchors. */
public interface PipelineCompilerDiagnostics {

    enum Severity {
        ERROR,
        WARNING,
        NOTE
    }

    void report(Severity severity, String message);

    default void error(String message) {
        report(Severity.ERROR, message);
    }

    default void warning(String message) {
        report(Severity.WARNING, message);
    }

    default void note(String message) {
        report(Severity.NOTE, message);
    }
}
