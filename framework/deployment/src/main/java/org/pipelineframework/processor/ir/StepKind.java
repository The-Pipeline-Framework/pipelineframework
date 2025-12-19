package org.pipelineframework.processor.ir;

/**
 * Enum representing different step types based on annotation configuration
 */
public enum StepKind {
    /** local=true - step runs in the same process */
    LOCAL,
    /** local=false - step communicates via gRPC */
    REMOTE
}