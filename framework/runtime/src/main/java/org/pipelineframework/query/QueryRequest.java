package org.pipelineframework.query;

/**
 * Request passed to a first-party framework query connector.
 */
public record QueryRequest<I>(
    QueryStepDescriptor descriptor,
    I input
) {
    public QueryRequest {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor must not be null");
        }
    }
}
