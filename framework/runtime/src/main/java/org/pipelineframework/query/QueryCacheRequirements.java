package org.pipelineframework.query;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.QueryCapabilities;

/**
 * Static cache constraints embedded in a generated provider-backed Query step.
 */
public record QueryCacheRequirements(
    ConnectorOperationIdentity operationIdentity,
    int providerMajorVersion,
    QueryCapabilities capabilities,
    Optional<Duration> negativeCacheTtl
) {
    public QueryCacheRequirements {
        operationIdentity = Objects.requireNonNull(operationIdentity, "query operation identity must not be null");
        if (providerMajorVersion < 1) {
            throw new IllegalArgumentException("query provider major version must be positive");
        }
        capabilities = Objects.requireNonNull(capabilities, "query capabilities must not be null");
        negativeCacheTtl = Objects.requireNonNull(negativeCacheTtl, "negative cache TTL must not be null");
        if (negativeCacheTtl.isPresent()) {
            Duration ttl = negativeCacheTtl.orElseThrow();
            if (ttl.isZero() || ttl.isNegative()) {
                throw new IllegalArgumentException("negative query cache TTL must be positive");
            }
            if (capabilities.maximumNegativeCacheTtl().isEmpty()) {
                throw new IllegalArgumentException(
                    "query operation " + operationIdentity + " does not support negative caching");
            }
            Duration maximum = capabilities.maximumNegativeCacheTtl().orElseThrow();
            if (ttl.compareTo(maximum) > 0) {
                throw new IllegalArgumentException(
                    "negative query cache TTL " + ttl + " exceeds operation " + operationIdentity
                        + " maximum " + maximum);
            }
        }
    }
}
