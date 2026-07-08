package org.pipelineframework.processor.ir;

/**
 * Identifies the reactive library used by an authored unary service boundary.
 */
public enum ReactiveReturnKind {
    MUTINY_UNI,
    REACTOR_MONO,
    COMPLETION_STAGE
}
