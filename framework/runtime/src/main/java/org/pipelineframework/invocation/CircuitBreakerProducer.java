package org.pipelineframework.invocation;

import java.time.Clock;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

import org.pipelineframework.runtime.core.resilience.CircuitBreaker;
import org.pipelineframework.runtime.core.resilience.InMemoryCircuitBreaker;

/**
 * Supplies the only currently supported local circuit implementation.
 */
@Singleton
final class CircuitBreakerProducer {

    @Produces
    @Singleton
    CircuitBreaker circuitBreaker(CircuitTelemetry telemetry) {
        return new InMemoryCircuitBreaker(Clock.systemUTC(), telemetry);
    }
}
