package org.pipelineframework.runtime.core.resilience;

import java.util.Objects;

/**
 * Result of asking a circuit whether an invocation may begin.
 */
public sealed interface CircuitDecision permits CircuitDecision.Permitted, CircuitDecision.Rejected {

    record Permitted(CircuitPermit permit) implements CircuitDecision {
        public Permitted {
            Objects.requireNonNull(permit, "permit must not be null");
        }
    }

    record Rejected(CircuitOpen open) implements CircuitDecision {
        public Rejected {
            Objects.requireNonNull(open, "open must not be null");
        }
    }
}
