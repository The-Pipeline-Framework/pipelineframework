package org.pipelineframework.connector.mcp;

import java.util.Objects;

import io.modelcontextprotocol.client.McpAsyncClient;
import org.pipelineframework.connector.ResolvedConnection;

/**
 * Runtime-only handle to a host-owned, initialized MCP client.
 *
 * <p>The host owns the client, its transport, any STDIO process, and their shutdown. The connector
 * borrows the client for one live invocation and never closes it.</p>
 */
public record McpClientConnection(McpAsyncClient client) implements ResolvedConnection {
    public McpClientConnection {
        client = Objects.requireNonNull(client, "MCP client must not be null");
        if (!client.isInitialized()) {
            throw new IllegalArgumentException("MCP client must be initialized by the host");
        }
    }
}
