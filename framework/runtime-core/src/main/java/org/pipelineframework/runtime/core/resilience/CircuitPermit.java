package org.pipelineframework.runtime.core.resilience;

import java.util.concurrent.CompletionStage;

/**
 * Admission token for exactly one dependency invocation.
 *
 * <p>Terminal methods are observer operations. Implementations must not let internal bookkeeping
 * failures replace the workload result, and callers must invoke exactly one terminal method.</p>
 */
public interface CircuitPermit {

    CompletionStage<Void> complete(CircuitOutcome outcome);

    default CompletionStage<Void> succeed() {
        return complete(CircuitOutcome.SUCCESS);
    }

    default CompletionStage<Void> healthFailure() {
        return complete(CircuitOutcome.HEALTH_FAILURE);
    }

    default CompletionStage<Void> neutral() {
        return complete(CircuitOutcome.NEUTRAL);
    }

    default CompletionStage<Void> cancel() {
        return complete(CircuitOutcome.NEUTRAL);
    }
}
