package org.pipelineframework.processor.ir;

/**
 * Distinguishes the authored service contract family for an internal step.
 */
public enum ServiceApiKind {
    REACTIVE,
    BLOCKING,
    BLOCKING_ITERATOR
}
