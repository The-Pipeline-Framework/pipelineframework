package org.pipelineframework.connector;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * Minimal cache guarantees declared by a unary Query operation.
 */
public record QueryCapabilities(
    QueryCacheability cacheability,
    Optional<Duration> maximumCacheAge,
    Optional<Duration> maximumNegativeCacheTtl
) {
    public QueryCapabilities {
        cacheability = Objects.requireNonNull(cacheability, "query cacheability must not be null");
        maximumCacheAge = positive(maximumCacheAge, "maximum query cache age");
        maximumNegativeCacheTtl = positive(maximumNegativeCacheTtl, "maximum negative query cache TTL");
        if (cacheability == QueryCacheability.LIVE_ONLY
            && (maximumCacheAge.isPresent() || maximumNegativeCacheTtl.isPresent())) {
            throw new IllegalArgumentException("live-only Query capabilities cannot declare cache TTL bounds");
        }
    }

    public static QueryCapabilities conservative() {
        return new QueryCapabilities(QueryCacheability.LIVE_ONLY, Optional.empty(), Optional.empty());
    }

    public static QueryCapabilities cacheable() {
        return new QueryCapabilities(QueryCacheability.CACHEABLE, Optional.empty(), Optional.empty());
    }

    private static Optional<Duration> positive(Optional<Duration> value, String label) {
        Objects.requireNonNull(value, label + " must not be null");
        value.ifPresent(duration -> {
            if (duration.isZero() || duration.isNegative()) {
                throw new IllegalArgumentException(label + " must be positive");
            }
        });
        return value;
    }
}
