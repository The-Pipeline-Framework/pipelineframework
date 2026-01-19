package org.pipelineframework.processor.ir;

/**
 * Enum representing the position of an aspect relative to a pipeline step.
 */
public enum AspectPosition {
    /**
     * Applies before the step execution. BEFORE_STEP aspects always execute before
     * the step, regardless of order value.
     */
    BEFORE_STEP,

    /**
     * Applies after the step execution. AFTER_STEP aspects always execute after
     * the step, regardless of order value.
     */
    AFTER_STEP
}