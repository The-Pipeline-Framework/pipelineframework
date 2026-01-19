package org.pipelineframework.processor.ir;

/**
 * Enum representing the scope of an aspect application.
 */
public enum AspectScope {
    /**
     * Applies to all steps present in the pipeline definition at generation time.
     * Future steps added later require regeneration.
     */
    GLOBAL,

    /**
     * Reserved for future extensions. In the current version, STEPS scope MUST be empty
     * or treated as GLOBAL with a warning.
     */
    STEPS
}