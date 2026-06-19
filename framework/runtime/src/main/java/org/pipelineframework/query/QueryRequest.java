package org.pipelineframework.query;

import java.util.Map;

/**
 * Request passed to a query connector.
 */
public record QueryRequest<I>(
    QueryStepDescriptor descriptor,
    I input,
    Map<String, Object> config
) {
    public QueryRequest {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor must not be null");
        }
        config = config == null ? Map.of() : Map.copyOf(config);
    }
}
