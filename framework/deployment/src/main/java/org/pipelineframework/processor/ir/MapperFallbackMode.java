package org.pipelineframework.processor.ir;

/**
 * Per-step mapper fallback strategy used when no explicit or inferred mapper matches.
 */
public enum MapperFallbackMode {
    NONE,
    JACKSON
}
