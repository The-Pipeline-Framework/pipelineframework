package org.pipelineframework.runtime.core.resilience;

import java.time.Duration;
import java.util.Objects;

/**
 * Circuit behavior and the scope that its implementation must guarantee.
 */
public record CircuitPolicy(
    CircuitScope requiredScope,
    int failureThreshold,
    Duration failureWindow,
    Duration openDuration,
    int halfOpenMaxPermits,
    Duration halfOpenRetryDelay,
    Duration halfOpenProbeLeaseDuration
) {

    /** Backwards-compatible policy form; a shared probe lease defaults to the open duration. */
    public CircuitPolicy(
        CircuitScope requiredScope,
        int failureThreshold,
        Duration failureWindow,
        Duration openDuration,
        int halfOpenMaxPermits,
        Duration halfOpenRetryDelay
    ) {
        this(requiredScope, failureThreshold, failureWindow, openDuration, halfOpenMaxPermits,
            halfOpenRetryDelay, openDuration);
    }

    public CircuitPolicy {
        Objects.requireNonNull(requiredScope, "requiredScope must not be null");
        if (failureThreshold < 1) {
            throw new IllegalArgumentException("failureThreshold must be greater than zero");
        }
        failureWindow = positive(failureWindow, "failureWindow");
        openDuration = positive(openDuration, "openDuration");
        if (halfOpenMaxPermits < 1) {
            throw new IllegalArgumentException("halfOpenMaxPermits must be greater than zero");
        }
        halfOpenRetryDelay = positive(halfOpenRetryDelay, "halfOpenRetryDelay");
        halfOpenProbeLeaseDuration = positive(halfOpenProbeLeaseDuration, "halfOpenProbeLeaseDuration");
    }

    private static Duration positive(Duration value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be greater than zero");
        }
        return value;
    }
}
