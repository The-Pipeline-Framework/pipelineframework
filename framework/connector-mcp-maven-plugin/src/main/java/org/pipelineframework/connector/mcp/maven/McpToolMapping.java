package org.pipelineframework.connector.mcp.maven;

import org.apache.maven.plugins.annotations.Parameter;

/** Explicit author decision that turns one discovered MCP tool into an imported TPF operation. */
public final class McpToolMapping {
    /** Empty means full projection; an explicit list is a strict dotted-property allowlist. */
    @Parameter
    java.util.List<String> includeFields = java.util.List.of();

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

    /** Required for a declared outputSchema; otherwise the canonical JsonPayload result is selected. */
    @Parameter
    String outputType;
}
