package org.pipelineframework.representation.spi;

/** Target-neutral shape of a named canonical type. */
public enum CanonicalTypeShape {
    RECORD,
    WRAPPER,
    ALIAS,
    UNION,
    UNKNOWN
}
