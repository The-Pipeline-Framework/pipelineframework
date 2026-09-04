package org.pipelineframework.connector.mcp;

import java.util.Objects;

import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;

/** Private execution mapping from one imported TPF identity to its exact MCP tool name. */
public record McpImportedTool(
    String mcpName,
    String operation,
    ConnectorOperationKind kind,
    int majorVersion,
    String inputType,
    String outputType
) {
    public McpImportedTool {
        mcpName = requireText(mcpName, "MCP tool name");
        operation = ConnectorProviderId.of(operation).value();
        kind = Objects.requireNonNull(kind, "MCP-backed operation kind must not be null");
        if (!kind.equals(ConnectorOperationKind.QUERY) && !kind.equals(ConnectorOperationKind.COMMAND)) {
            throw new IllegalArgumentException("MCP-backed operation kind must be query or command");
        }
        if (majorVersion < 1) {
            throw new IllegalArgumentException("MCP-backed operation major version must be positive");
        }
        inputType = requireText(inputType, "MCP-backed input type");
        outputType = requireText(outputType, "MCP-backed output type");
    }

    private static String requireText(String value, String subject) {
        String result = Objects.requireNonNull(value, subject + " must not be null").trim();
        if (result.isEmpty()) {
            throw new IllegalArgumentException(subject + " must not be blank");
        }
        return result;
    }
}
