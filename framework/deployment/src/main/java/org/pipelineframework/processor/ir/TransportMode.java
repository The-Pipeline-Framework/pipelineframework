package org.pipelineframework.processor.ir;

/**
 * Transport mode used for orchestrator client generation.
 */
public enum TransportMode {
    GRPC,
    REST,
    LOCAL;

    public static TransportMode fromString(String raw) {
        if (raw == null || raw.isBlank()) {
            return GRPC;
        }
        String normalized = raw.trim();
        if ("REST".equalsIgnoreCase(normalized)) {
            return REST;
        }
        if ("LOCAL".equalsIgnoreCase(normalized)) {
            return LOCAL;
        }
        return GRPC;
    }
}
