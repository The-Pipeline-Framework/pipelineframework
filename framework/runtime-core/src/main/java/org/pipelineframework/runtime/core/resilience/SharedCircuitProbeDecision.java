package org.pipelineframework.runtime.core.resilience;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/** Result of an authoritative shared HALF_OPEN admission attempt. */
public record SharedCircuitProbeDecision(
    SharedCircuitSnapshot snapshot,
    Optional<SharedCircuitProbe> probe,
    Instant notBefore
) {
    public SharedCircuitProbeDecision {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        probe = Objects.requireNonNull(probe, "probe must not be null");
        Objects.requireNonNull(notBefore, "notBefore must not be null");
    }

    public boolean permitted() {
        return probe.isPresent();
    }
}
