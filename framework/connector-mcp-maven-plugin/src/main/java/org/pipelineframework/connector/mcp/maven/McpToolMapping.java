package org.pipelineframework.connector.mcp.maven;

import org.apache.maven.plugins.annotations.Parameter;

/** Explicit author decision that turns one discovered MCP tool into an imported TPF operation. */
public final class McpToolMapping {
    @Parameter(required = true)
    String mcpName;

    @Parameter(required = true)
    String operation;

    @Parameter(required = true)
    String kind;

    @Parameter(defaultValue = "1")
    int majorVersion;

    @Parameter(required = true)
    String inputType;

    @Parameter(required = true)
    String outputType;
}
