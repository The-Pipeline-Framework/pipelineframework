package org.pipelineframework.processor.ir;

import java.util.Locale;

/**
 * Binding model for orchestrator renderers.
 */
public record OrchestratorBinding(
    PipelineStepModel model,
    String basePackage,
    String transport,
    String inputTypeName,
    String outputTypeName,
    boolean inputStreaming,
    boolean outputStreaming
) implements PipelineBinding {
    public String normalizedTransport() {
        if (transport == null) {
            return "GRPC";
        }
        String trimmed = transport.trim();
        return trimmed.isEmpty() ? "GRPC" : trimmed.toUpperCase(Locale.ROOT);
    }
}
