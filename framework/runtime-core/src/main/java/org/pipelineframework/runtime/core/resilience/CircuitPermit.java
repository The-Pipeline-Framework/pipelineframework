package org.pipelineframework.runtime.core.resilience;

/**
 * Admission token for exactly one dependency invocation.
 *
 * <p>Terminal methods are observer operations. Implementations must not let internal bookkeeping
 * failures replace the workload result, and callers must invoke exactly one terminal method.</p>
 */
public interface CircuitPermit {

    void complete(CircuitOutcome outcome);

    default void succeed() {
        complete(CircuitOutcome.SUCCESS);
    }

    default void healthFailure() {
        complete(CircuitOutcome.HEALTH_FAILURE);
    }

    default void neutral() {
        complete(CircuitOutcome.NEUTRAL);
    }

    default void cancel() {
        complete(CircuitOutcome.NEUTRAL);
    }
}
