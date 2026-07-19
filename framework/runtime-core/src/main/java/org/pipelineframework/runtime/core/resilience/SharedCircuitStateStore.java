package org.pipelineframework.runtime.core.resilience;

import java.time.Instant;
import java.util.concurrent.CompletionStage;

/**
 * Narrow authority SPI for one shared dependency circuit domain.
 *
 * <p>Implementations own authoritative state, policy-fingerprint validation, and probe leases.
 * This is deliberately not a general distributed-state abstraction and is independent from durable
 * execution state.</p>
 */
public interface SharedCircuitStateStore {

    CompletionStage<SharedCircuitSnapshot> read(CircuitIdentity identity, CircuitPolicy policy);

    CompletionStage<SharedCircuitSnapshot> recordHealthFailures(
        CircuitIdentity identity,
        CircuitPolicy policy,
        int failures,
        Instant observedAt);

    CompletionStage<SharedCircuitProbeDecision> acquireHalfOpenProbe(
        CircuitIdentity identity,
        CircuitPolicy policy,
        String owner,
        Instant now);

    CompletionStage<Void> completeProbe(
        CircuitIdentity identity,
        CircuitPolicy policy,
        SharedCircuitProbe probe,
        CircuitOutcome outcome,
        Instant completedAt);
}
