package org.pipelineframework.connector.mcp;

import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;

/** Private immutable identity, original schemas and projection of one imported MCP operation. */
public record McpImportedTool(
    String mcpName,
    String operation,
    ConnectorOperationKind kind,
    int majorVersion,
    String inputType,
    String outputType,
    McpJsonSchema inputSchema,
    Optional<McpJsonSchema> outputSchema,
    String projectionId,
    ResultMode resultMode
) {
    public static final String PROJECTION_V1 = "tpf-mcp-importer-v1";
    public static final String JSON_PAYLOAD = "<tpf.connector.JsonPayload>";

    public enum ResultMode {
        STRUCTURED("structured"), JSON_PAYLOAD("json-payload");

        private final String value;

        ResultMode(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        public static ResultMode from(String value) {
            return java.util.Arrays.stream(values()).filter(mode -> mode.value.equals(value)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("unsupported MCP result mode"));
        }
    }

    public McpImportedTool {
        mcpName = requireText(mcpName, "MCP tool name");
        operation = ConnectorProviderId.of(operation).value();
        kind = Objects.requireNonNull(kind, "MCP-backed operation kind");
        if (!kind.equals(ConnectorOperationKind.QUERY) && !kind.equals(ConnectorOperationKind.COMMAND)) {
            throw new IllegalArgumentException("MCP-backed operation kind must be query or command");
        }
        if (majorVersion < 1) {
            throw new IllegalArgumentException("MCP-backed operation major version must be positive");
        }
        inputType = requireText(inputType, "MCP-backed input type");
        outputType = requireText(outputType, "MCP-backed output type");
        Objects.requireNonNull(inputSchema, "inputSchema");
        Objects.requireNonNull(outputSchema, "outputSchema");
        if (outputSchema.stream().anyMatch(schema -> !schema.includeFields().isEmpty())) {
            throw new IllegalArgumentException("MCP output schema cannot have an input selection");
        }
        Objects.requireNonNull(resultMode, "resultMode");
        if (!PROJECTION_V1.equals(projectionId)) {
            throw new IllegalArgumentException("unsupported MCP projection identity; refresh with a compatible importer");
        }
        if ((resultMode == ResultMode.STRUCTURED) != outputSchema.isPresent()
            || (resultMode == ResultMode.JSON_PAYLOAD) != JSON_PAYLOAD.equals(outputType)) {
            throw new IllegalArgumentException("MCP result mode disagrees with output schema or canonical output type");
        }
    }

    public String identity() {
        return kind.value() + ":" + operation + ":" + majorVersion;
    }

    /** Digest covers schemas AND the exact author selection/projection, including result mode. */
    public String sha256() {
        return McpPinnedJson.sha256(McpPinnedJson.canonicalize(fields()));
    }

    public ObjectNode toJson() {
        return fields().put("pinSha256", sha256());
    }

    private ObjectNode fields() {
        var result = JsonNodeFactory.instance.objectNode();
        result.put("mcpName", mcpName);
        result.put("operation", operation);
        result.put("kind", kind.value());
        result.put("majorVersion", majorVersion);
        result.put("input", inputType);
        result.put("output", outputType);
        result.set("inputSchema", inputSchema.node());
        result.put("inputSchemaSha256", inputSchema.sha256());
        outputSchema.ifPresent(schema -> {
            result.set("outputSchema", schema.node());
            result.put("outputSchemaSha256", schema.sha256());
        });
        result.put("projectionId", projectionId);
        if (!inputSchema.includeFields().isEmpty()) {
            var fields = result.putArray("includeFields");
            inputSchema.includeFields().forEach(fields::add);
        }
        result.put("resultMode", resultMode.value());
        return result;
    }

    private static String requireText(String value, String subject) {
        String result = Objects.requireNonNull(value, subject).trim();
        if (result.isEmpty()) {
            throw new IllegalArgumentException(subject + " must not be blank");
        }
        return result;
    }
}
