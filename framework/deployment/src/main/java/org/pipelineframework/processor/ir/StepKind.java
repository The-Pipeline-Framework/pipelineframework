package org.pipelineframework.processor.ir;

/**
 * Enum representing different step types based on annotation configuration
 */
public enum StepKind {
    /** step runs in the same process */
    LOCAL,
    /** step communicates remotely via a protocol binding */
    REMOTE
}
