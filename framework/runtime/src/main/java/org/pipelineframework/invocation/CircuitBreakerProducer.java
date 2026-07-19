package org.pipelineframework.invocation;

import java.time.Clock;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

import org.pipelineframework.config.PipelineResilienceConfig;
import org.pipelineframework.runtime.core.resilience.CircuitBreaker;
import org.pipelineframework.runtime.core.resilience.InMemoryCircuitBreaker;
import org.pipelineframework.runtime.core.resilience.ScopeRoutingCircuitBreaker;
import org.pipelineframework.runtime.core.resilience.SharedCircuitBreaker;

/**
 * Supplies scope-specific circuit implementations without presenting local protection as shared.
 */
@Singleton
final class CircuitBreakerProducer {

    @Produces
    @Singleton
    CircuitBreaker circuitBreaker(CircuitTelemetry telemetry, PipelineResilienceConfig resilienceConfig) {
        Clock clock = Clock.systemUTC();
        CircuitBreaker local = new InMemoryCircuitBreaker(clock, telemetry);
        CircuitBreaker shared = new SharedCircuitBreaker(
            new DynamoSharedCircuitStateStore(resilienceConfig.shared(), clock),
            clock,
            resilienceConfig.shared().maxStateStaleness(),
            resilienceConfig.shared().backendRetryDelay(),
            telemetry);
        return new ScopeRoutingCircuitBreaker(local, shared);
    }
}
