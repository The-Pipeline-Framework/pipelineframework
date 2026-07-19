package org.pipelineframework.runtime.core.resilience;

/**
 * Admits dependency calls and records their result. Implementations must reject policies whose
 * required scope they cannot guarantee.
 */
public interface CircuitBreaker {

    /**
     * Acquires permission to invoke a dependency under the requested policy.
     *
     * @throws IllegalArgumentException when the implementation cannot guarantee the policy's
     *         requested scope
     */
    CircuitDecision acquire(CircuitIdentity identity, CircuitPolicy policy);
}
