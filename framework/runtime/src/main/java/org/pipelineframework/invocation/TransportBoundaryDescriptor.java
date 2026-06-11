package org.pipelineframework.invocation;

import java.util.Objects;

/**
 * Diagnostic identity for a transport boundary crossed by generated client steps or transition workers.
 */
public record TransportBoundaryDescriptor(String protocol, String target) {

    public TransportBoundaryDescriptor {
        protocol = requireText(protocol, "protocol");
        target = requireText(target, "target");
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return trimmed;
    }
}
