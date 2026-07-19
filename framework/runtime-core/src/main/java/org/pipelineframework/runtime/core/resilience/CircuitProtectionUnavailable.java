package org.pipelineframework.runtime.core.resilience;

import java.time.Instant;
import java.util.Objects;

/**
 * A shared circuit cannot safely establish its required protection guarantee.
 *
 * <p>The {@code notBefore} value is a scheduling hint for a later protection lookup. It is not a
 * permit to invoke the dependency.</p>
 */
public record CircuitProtectionUnavailable(
    CircuitIdentity identity,
    CircuitScope scope,
    Instant notBefore
) {
    public CircuitProtectionUnavailable {
        Objects.requireNonNull(identity, "identity must not be null");
        Objects.requireNonNull(scope, "scope must not be null");
        Objects.requireNonNull(notBefore, "notBefore must not be null");
    }
}
