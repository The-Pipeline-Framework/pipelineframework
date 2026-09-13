package org.pipelineframework.config.pipeline;

/** Explicit application callback capability and bean selections. */
public record PipelineYamlAwaitCallback(String name, String endpointResolver, String authenticator) {
    public PipelineYamlAwaitCallback {
        name = org.pipelineframework.connector.ConnectorProviderId.of(name).value();
        java.util.Objects.requireNonNull(endpointResolver, "endpointResolver");
        java.util.Objects.requireNonNull(authenticator, "authenticator");
        if (endpointResolver.isBlank() || authenticator.isBlank()) {
            throw new IllegalArgumentException("callback endpointResolver and authenticator must be declared");
        }
    }
}
