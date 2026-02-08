package org.pipelineframework.processor.ir;

import java.util.Optional;

/**
 * Transport mode used for orchestrator client generation.
 */
public enum TransportMode {
    GRPC,
    REST,
    LOCAL;

    /**
     * The platform-specific client step class name suffix for this transport mode.
     *
     * @return the client step suffix corresponding to the transport mode — one of
     *         "GrpcClientStep", "RestClientStep", or "LocalClientStep"
     */
    public String clientStepSuffix() {
        return switch (this) {
            case REST -> "RestClientStep";
            case LOCAL -> "LocalClientStep";
            case GRPC -> "GrpcClientStep";
        };
    }

    /**
     * Parse a string into a TransportMode when the input matches a known mode name.
     *
     * @param raw the input string to parse; null or blank input is treated as no match
     * @return an Optional containing the matching TransportMode for "REST", "LOCAL", or "GRPC" (case-insensitive), or Optional.empty() if the input is null, blank, or does not match
     */
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

    /**
     * Parse the given string into a TransportMode, using GRPC as the default when the input is null or blank.
     *
     * @param raw the input string to parse; may be null or blank
     * @return the corresponding TransportMode, or GRPC if {@code raw} is null or blank
     * @throws IllegalArgumentException if {@code raw} is non-blank and does not match GRPC, REST, or LOCAL
     */
    public static TransportMode fromString(String raw) {
        if (raw == null || raw.isBlank()) {
            return GRPC;
        }
        return fromStringOptional(raw)
            .orElseThrow(() -> new IllegalArgumentException(
                "Unknown transport mode: '" + raw + "'; expected GRPC|gRPC|REST|LOCAL"));
    }
}