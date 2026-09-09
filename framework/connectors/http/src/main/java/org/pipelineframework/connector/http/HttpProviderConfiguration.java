package org.pipelineframework.connector.http;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.connector.ConnectionRef;

/** Application-owned logical connection and bounded transport limits. */
public record HttpProviderConfiguration(
    ConnectionRef connection,
    Optional<Integer> maxRequestBytes,
    Optional<Integer> maxResponseBytes,
    Optional<Duration> requestTimeout
) {
    public static final int DEFAULT_MAX_REQUEST_BYTES = 1024 * 1024;
    public static final int DEFAULT_MAX_RESPONSE_BYTES = 4 * 1024 * 1024;
    public static final int MAX_BYTES = 32 * 1024 * 1024;
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    public HttpProviderConfiguration {
        connection = Objects.requireNonNull(connection, "HTTP connection reference must not be null");
        maxRequestBytes = bounded(maxRequestBytes, "maximum HTTP request bytes");
        maxResponseBytes = bounded(maxResponseBytes, "maximum HTTP response bytes");
        requestTimeout = Objects.requireNonNull(requestTimeout, "HTTP request timeout must not be null");
        requestTimeout.ifPresent(value -> {
            if (value.isZero() || value.isNegative() || value.compareTo(Duration.ofMinutes(10)) > 0) {
                throw new IllegalArgumentException(
                    "HTTP request timeout must be greater than zero and at most ten minutes");
            }
        });
    }

    public int effectiveMaxRequestBytes() {
        return maxRequestBytes.orElse(DEFAULT_MAX_REQUEST_BYTES);
    }

    public int effectiveMaxResponseBytes() {
        return maxResponseBytes.orElse(DEFAULT_MAX_RESPONSE_BYTES);
    }

    public Duration effectiveRequestTimeout() {
        return requestTimeout.orElse(DEFAULT_TIMEOUT);
    }

    private static Optional<Integer> bounded(Optional<Integer> value, String subject) {
        Optional<Integer> result = Objects.requireNonNull(value, subject + " must not be null");
        result.ifPresent(limit -> {
            if (limit < 1 || limit > MAX_BYTES) {
                throw new IllegalArgumentException(subject + " must be between 1 and " + MAX_BYTES);
            }
        });
        return result;
    }
}
