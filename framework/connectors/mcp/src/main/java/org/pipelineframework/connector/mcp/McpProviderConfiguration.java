package org.pipelineframework.connector.mcp;

import java.util.Objects;

import org.pipelineframework.connector.ConnectionRef;

/** Deployment-owned reference to a host-managed MCP connection. */
public record McpProviderConfiguration(ConnectionRef connection) {
    public McpProviderConfiguration {
        connection = Objects.requireNonNull(connection, "MCP connection reference must not be null");
    }
}
