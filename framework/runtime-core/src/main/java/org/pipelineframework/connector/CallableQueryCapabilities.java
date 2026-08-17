package org.pipelineframework.connector;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/** Model-safe Query cache semantics. */
public record CallableQueryCapabilities(
    QueryCacheability cacheability,
    Optional<Duration> maximumCacheAge,
    Optional<Duration> maximumNegativeCacheTtl
) {
    public CallableQueryCapabilities {
        cacheability = Objects.requireNonNull(cacheability, "query cacheability must not be null");
        maximumCacheAge = Objects.requireNonNull(maximumCacheAge, "maximum cache age must not be null");
        maximumNegativeCacheTtl = Objects.requireNonNull(
            maximumNegativeCacheTtl, "maximum negative cache TTL must not be null");
    }

    public static CallableQueryCapabilities from(QueryCapabilities capabilities) {
        Objects.requireNonNull(capabilities, "query capabilities must not be null");
        return new CallableQueryCapabilities(
            capabilities.cacheability(), capabilities.maximumCacheAge(), capabilities.maximumNegativeCacheTtl());
    }
}
