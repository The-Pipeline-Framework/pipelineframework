package org.pipelineframework.connector;

/**
 * Host-neutral external-JAR construction seam, discoverable through {@link java.util.ServiceLoader}.
 */
public interface ConnectorProviderFactory {
    ConnectorProvider<?> create();
}
