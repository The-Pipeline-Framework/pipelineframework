package org.pipelineframework.runtime.core.resilience;

import java.util.Objects;

/**
 * Result of asking a circuit whether an invocation may begin.
 */
public sealed interface CircuitDecision permits CircuitDecision.Permitted, CircuitDecision.Rejected,
    CircuitDecision.Unavailable {

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

    /**
     * The requested protection scope could not be consulted safely. No dependency call may start.
     */
    record Unavailable(CircuitProtectionUnavailable protection) implements CircuitDecision {
        public Unavailable {
            Objects.requireNonNull(protection, "protection must not be null");
        }
    }
}
