package org.pipelineframework.stream;

import java.util.Optional;

/** Provider-owned durable source position. Core may persist and compare it, but must not parse it. */
public record OpaqueSourceCheckpoint(Optional<String> value) {
    public OpaqueSourceCheckpoint {
        value = value == null ? Optional.empty() : value.map(candidate -> {
            if (candidate.isBlank()) {
                throw new IllegalArgumentException("checkpoint must not be blank when present");
            }
            return candidate;
        });
    }

    public static OpaqueSourceCheckpoint initial() {
        return new OpaqueSourceCheckpoint(Optional.empty());
    }
}
