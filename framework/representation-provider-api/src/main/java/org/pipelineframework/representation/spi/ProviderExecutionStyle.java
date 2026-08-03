package org.pipelineframework.representation.spi;

/** The neutral service style a provider-generated pipeline facade exposes to the host. */
public enum ProviderExecutionStyle {
    REACTIVE,
    BLOCKING,
    BLOCKING_ITERATOR
}
