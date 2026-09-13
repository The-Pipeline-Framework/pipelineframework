package org.pipelineframework.processor.ir;

import java.util.Locale;

/**
 * Binding model for orchestrator renderers.
 *
 * @param model the pipeline step model describing the orchestrator entrypoint
 * @param basePackage the base package for generated orchestrator artifacts
 * @param transport the configured transport name
 * @param inputTypeName the input type name for the orchestrator entrypoint
 * @param outputTypeName the output type name for the orchestrator entrypoint
 * @param inputStreaming whether the orchestrator accepts streaming input
 * @param outputStreaming whether the orchestrator produces streaming output
 * @param firstStepServiceName the service name of the first pipeline step
 * @param firstStepStreamingShape the streaming shape of the first pipeline step
 * @param cliName the CLI application name
 * @param cliDescription the CLI description text
 * @param cliVersion the CLI version string
 */
public record OrchestratorBinding(
    PipelineStepModel model,
    String basePackage,
    String transport,
    String inputTypeName,
    String outputTypeName,
    boolean inputStreaming,
    boolean outputStreaming,
    String firstStepServiceName,
    StreamingShape firstStepStreamingShape,
    String cliName,
    String cliDescription,
    String cliVersion
) implements PipelineBinding {
    /**
     * Normalize the configured transport to an uppercase token.
     *
     * @return the normalized transport string, defaulting to GRPC
     */
    public String normalizedTransport() {
        if (transport == null) {
            return "GRPC";
        }
        String trimmed = transport.trim();
        return trimmed.isEmpty() ? "GRPC" : trimmed.toUpperCase(Locale.ROOT);
    }
}
