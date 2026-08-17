package org.pipelineframework.cache;

import java.util.Objects;

/**
 * Internal cache representation of an explicitly cacheable Query NotFound observation.
 */
public record QueryNotFoundCacheEntry(String outcomeCode) {
    public QueryNotFoundCacheEntry {
        outcomeCode = Objects.requireNonNull(outcomeCode, "query outcome code must not be null");
        if (!outcomeCode.matches("[a-z][a-z0-9-]{0,127}")) {
            throw new IllegalArgumentException("invalid query outcome code: " + outcomeCode);
        }
    }
}
