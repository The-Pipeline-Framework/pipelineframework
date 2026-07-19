package org.pipelineframework.runtime.core.resilience;

import java.util.Objects;

/**
 * Stable identity of the logical dependency protected by a circuit.
 *
 * <p>Scope is deliberately not part of this value. It is a policy and implementation guarantee,
 * not a second dependency name.</p>
 */
public record CircuitIdentity(String value) {

    public CircuitIdentity {
        Objects.requireNonNull(value, "value must not be null");
        value = value.trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException("value must not be blank");
        }
    }
}
