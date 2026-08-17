package org.pipelineframework.connector;

/**
 * Whether a Query operation permits its observations to participate in pipeline cache replay.
 */
public enum QueryCacheability {
    /** Every invocation must observe the provider live. */
    LIVE_ONLY,
    /** Observations may be reused through the existing pipeline cache policies. */
    CACHEABLE
}
