package org.pipelineframework.processor.ir;

import java.util.Optional;

/**
 * Transport mode used for orchestrator client generation.
 */
public enum TransportMode {
    GRPC,
    REST,
    LOCAL;

    public String clientStepSuffix() {
        return switch (this) {
            case REST -> "RestClientStep";
            case LOCAL -> "LocalClientStep";
            case GRPC -> "GrpcClientStep";
        };
    }

    public static Optional<TransportMode> fromStringOptional(String raw) {
        if (raw == null || raw.isBlank()) {
            return Optional.empty();
        }
        String normalized = raw.trim();
        if ("REST".equalsIgnoreCase(normalized)) {
            return Optional.of(REST);
        }
        if ("LOCAL".equalsIgnoreCase(normalized)) {
            return Optional.of(LOCAL);
        }
        if ("GRPC".equalsIgnoreCase(normalized)) {
            return Optional.of(GRPC);
        }
        return Optional.empty();
    }

    public static TransportMode fromString(String raw) {
        if (raw == null || raw.isBlank()) {
            return GRPC;
        }
        return fromStringOptional(raw)
            .orElseThrow(() -> new IllegalArgumentException(
                "Unknown transport mode: '" + raw + "'; expected GRPC|gRPC|REST|LOCAL"));
    }
}
