package org.pipelineframework.processor;

import java.util.Objects;

/** Identifies the compiler phase that failed while preserving its original exception. */
final class PipelineCompilationException extends RuntimeException {

    private final String phaseName;

    PipelineCompilationException(String phaseName, Throwable cause) {
        super(Objects.requireNonNull(cause, "cause must not be null"));
        this.phaseName = Objects.requireNonNull(phaseName, "phaseName must not be null");
    }

    String phaseName() {
        return phaseName;
    }
}
