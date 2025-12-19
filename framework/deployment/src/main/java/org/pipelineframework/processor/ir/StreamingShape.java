package org.pipelineframework.processor.ir;

/**
 * Enum representing different streaming patterns for reactive services
 */
public enum StreamingShape {
    /** OneToOne: Input -> Uni&lt;Output&gt; */
    UNARY_UNARY,
    /** OneToMany: Input -> Multi&lt;Output&gt; */
    UNARY_STREAMING,
    /** ManyToOne: Multi&lt;Input&gt; -> Uni&lt;Output&gt; */
    STREAMING_UNARY,
    /** ManyToMany: Multi&lt;Input&gt; -> Multi&lt;Output&gt; */
    STREAMING_STREAMING
}