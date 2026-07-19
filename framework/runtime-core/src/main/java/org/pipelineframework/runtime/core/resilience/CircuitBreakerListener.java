package org.pipelineframework.runtime.core.resilience;

/**
 * Observes circuit state transitions without coupling the core to a telemetry implementation.
 */
@FunctionalInterface
public interface CircuitBreakerListener {
    CircuitBreakerListener NOOP = (identity, scope, transition) -> {
    };

    void onTransition(CircuitIdentity identity, CircuitScope scope, CircuitStateTransition transition);
}
