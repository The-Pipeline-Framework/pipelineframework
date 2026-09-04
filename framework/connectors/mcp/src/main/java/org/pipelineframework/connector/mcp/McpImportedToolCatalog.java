package org.pipelineframework.connector.mcp;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.ConnectorOperationKind;

/** Strict reader for the private, release-pinned MCP execution mapping. */
public final class McpImportedToolCatalog {
    public static final String RESOURCE_PATH = "META-INF/pipeline/mcp-tools.json";
    private static final ObjectMapper JSON = PipelineJson.mapper();
    private final List<McpImportedTool> tools;

    private McpImportedToolCatalog(List<McpImportedTool> tools) {
        this.tools = List.copyOf(tools);
    }

    public static McpImportedToolCatalog load(ClassLoader classLoader) {
        Objects.requireNonNull(classLoader, "MCP import class loader must not be null");
        try {
            Enumeration<URL> resources = classLoader.getResources(RESOURCE_PATH);
            List<URL> ordered = new ArrayList<>();
            while (resources.hasMoreElements()) {
                ordered.add(resources.nextElement());
            }
            ordered.sort(Comparator.comparing(URL::toExternalForm));
            Map<String, McpImportedTool> imported = new LinkedHashMap<>();
            for (URL resource : ordered) {
                for (McpImportedTool tool : read(resource)) {
                    String identity = tool.kind().value() + ":" + tool.operation() + ":" + tool.majorVersion();
                    if (imported.putIfAbsent(identity, tool) != null) {
                        throw new IllegalArgumentException("duplicate pinned MCP operation identity: " + identity);
                    }
                }
            }
            return new McpImportedToolCatalog(imported.values().stream()
                .sorted(Comparator.comparing(McpImportedTool::operation)
                    .thenComparing(tool -> tool.kind().value()).thenComparingInt(McpImportedTool::majorVersion))
                .toList());
        } catch (IOException failure) {
            throw new IllegalStateException("unable to load pinned MCP tool mappings", failure);
        }
    }

    public List<McpImportedTool> tools() {
        return tools;
    }

    private static List<McpImportedTool> read(URL resource) throws IOException {
        JsonNode root = JSON.readTree(resource);
        if (root.path("schemaVersion").asInt(-1) != 1 || !"mcp.client".equals(root.path("provider").asText())) {
            throw new IllegalArgumentException("invalid pinned MCP tool catalog at " + resource);
        }
        if (!root.path("tools").isArray()) {
            throw new IllegalArgumentException("pinned MCP tool catalog has no tools array at " + resource);
        }
        List<McpImportedTool> result = new ArrayList<>();
        for (JsonNode tool : root.path("tools")) {
            result.add(new McpImportedTool(
                required(tool, "mcpName"), required(tool, "operation"),
                ConnectorOperationKind.of(required(tool, "kind")), tool.path("majorVersion").asInt(-1),
                required(tool, "input"), required(tool, "output")));
        }
        return result;
    }

    private static String required(JsonNode value, String field) {
        JsonNode result = value.get(field);
        if (result == null || !result.isTextual() || result.textValue().isBlank()) {
            throw new IllegalArgumentException("pinned MCP tool field '" + field + "' must be a non-blank string");
        }
        return result.textValue();
    }
}
