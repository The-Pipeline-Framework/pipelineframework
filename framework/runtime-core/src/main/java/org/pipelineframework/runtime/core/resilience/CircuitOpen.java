package org.pipelineframework.runtime.core.resilience;

import java.time.Instant;
import java.util.Objects;

/**
 * A refusal to start an invocation because the circuit cannot currently admit it.
 *
 * <p>{@code notBefore} is a lower-bound scheduling hint, not a promise that a later acquisition
 * will be permitted. Another caller can consume a half-open permit or the dependency can fail and
 * reopen the circuit before that instant.</p>
 */
public record CircuitOpen(CircuitIdentity identity, CircuitScope scope, Instant notBefore) {

    public CircuitOpen {
        Objects.requireNonNull(identity, "identity must not be null");
        Objects.requireNonNull(scope, "scope must not be null");
        Objects.requireNonNull(notBefore, "notBefore must not be null");
    }
}
