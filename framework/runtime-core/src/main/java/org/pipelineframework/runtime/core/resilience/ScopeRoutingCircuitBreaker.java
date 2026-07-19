package org.pipelineframework.runtime.core.resilience;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** Routes policy scope to the backend that can honestly provide that scope's guarantees. */
public final class ScopeRoutingCircuitBreaker implements CircuitBreaker {
    private final CircuitBreaker local;
    private final CircuitBreaker shared;

    public ScopeRoutingCircuitBreaker(CircuitBreaker local, CircuitBreaker shared) {
        this.local = Objects.requireNonNull(local, "local must not be null");
        this.shared = Objects.requireNonNull(shared, "shared must not be null");
    }

    @Override
    public CompletionStage<CircuitDecision> acquire(CircuitIdentity identity, CircuitPolicy policy) {
        Objects.requireNonNull(policy, "policy must not be null");
        return switch (policy.requiredScope()) {
            case LOCAL_PROCESS -> local.acquire(identity, policy);
            case SHARED_DEPENDENCY -> shared.acquire(identity, policy);
        };
    }
}
