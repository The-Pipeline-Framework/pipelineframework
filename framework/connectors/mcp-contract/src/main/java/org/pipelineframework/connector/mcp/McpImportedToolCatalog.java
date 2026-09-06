package org.pipelineframework.connector.mcp;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.pipelineframework.connector.ConnectorOperationKind;

/** Strict, bounded reader/writer for private release pins; never performs live MCP discovery. */
public final class McpImportedToolCatalog {
    public static final String RESOURCE_PATH = "META-INF/pipeline/mcp-tools.json";
    public static final int SCHEMA_VERSION = 2;
    private static final int MAX_TOOLS = 128;
    private final List<McpImportedTool> tools;

    public McpImportedToolCatalog(List<McpImportedTool> tools) {
        if (tools.size() > MAX_TOOLS) {
            throw new IllegalArgumentException("too many imported MCP tools");
        }
        Map<String, McpImportedTool> imported = new LinkedHashMap<>();
        for (McpImportedTool tool : tools) {
            McpImportedTool previous = imported.putIfAbsent(tool.identity(), tool);
            if (previous != null && !previous.equals(tool)) {
                throw new IllegalArgumentException("conflicting pinned MCP operation identity: " + tool.identity());
            }
        }
        this.tools = imported.values().stream().sorted(Comparator.comparing(McpImportedTool::identity)).toList();
    }

    public static McpImportedToolCatalog load(ClassLoader classLoader) {
        Objects.requireNonNull(classLoader, "MCP import class loader");
        try {
            var resources = classLoader.getResources(RESOURCE_PATH);
            List<URL> ordered = new ArrayList<>();
            while (resources.hasMoreElements()) {
                if (ordered.size() >= 64) {
                    throw new IllegalArgumentException("too many MCP pin resources");
                }
                ordered.add(resources.nextElement());
            }
            ordered.sort(Comparator.comparing(URL::toExternalForm));
            List<McpImportedTool> imported = new ArrayList<>();
            for (URL resource : ordered) {
                try (var stream = resource.openStream()) {
                    byte[] bytes = stream.readNBytes(McpPinnedJson.MAX_BYTES + 1);
                    if (bytes.length > McpPinnedJson.MAX_BYTES) {
                        throw new IllegalArgumentException("MCP pin resource exceeds size limit");
                    }
                    imported.addAll(read(new String(bytes, StandardCharsets.UTF_8)).tools());
                }
            }
            return new McpImportedToolCatalog(imported);
        } catch (IOException failure) {
            throw new IllegalStateException("unable to load pinned MCP tool mappings", failure);
        }
    }

    public List<McpImportedTool> tools() {
        return tools;
    }

    public String json() {
        var root = JsonNodeFactory.instance.objectNode();
        root.put("schemaVersion", SCHEMA_VERSION);
        root.put("provider", "mcp.client");
        var array = root.putArray("tools");
        tools.forEach(tool -> array.add(tool.toJson()));
        return McpPinnedJson.canonicalize(root) + "\n";
    }

    public static McpImportedToolCatalog read(String json) {
        JsonNode root = McpPinnedJson.parse(json);
        fields(root, Set.of("schemaVersion", "provider", "tools"));
        if (!root.path("schemaVersion").isIntegralNumber() || !root.path("schemaVersion").canConvertToInt()
            || root.path("schemaVersion").intValue() != SCHEMA_VERSION
            || !"mcp.client".equals(required(root, "provider"))) {
            throw new IllegalArgumentException("unsupported MCP pin version; explicitly refresh the MCP import");
        }
        if (!root.path("tools").isArray() || root.path("tools").size() > MAX_TOOLS) {
            throw new IllegalArgumentException("pinned MCP tools must be a bounded array");
        }
        List<McpImportedTool> result = new ArrayList<>();
        for (JsonNode tool : root.path("tools")) {
            fields(tool, Set.of("mcpName", "operation", "kind", "majorVersion", "input", "output",
                "inputSchema", "inputSchemaSha256", "outputSchema", "outputSchemaSha256", "projectionId",
                "resultMode", "pinSha256", "includeFields"));
            if (!tool.path("majorVersion").isIntegralNumber() || !tool.path("majorVersion").canConvertToInt()) {
                throw new IllegalArgumentException("MCP operation majorVersion must be an integer");
            }
            List<String> selection = new ArrayList<>();
            if (tool.has("includeFields")) {
                if (!tool.get("includeFields").isArray()) {
                    throw new IllegalArgumentException("MCP includeFields must be an array");
                }
                for (JsonNode path : tool.get("includeFields")) {
                    if (!path.isTextual()) {
                        throw new IllegalArgumentException("MCP includeFields paths must be strings");
                    }
                    selection.add(path.textValue());
                }
            }
            McpJsonSchema input = schema(tool, "inputSchema", selection);
            Optional<McpJsonSchema> output = tool.has("outputSchema")
                ? Optional.of(schema(tool, "outputSchema", List.of())) : Optional.empty();
            if (output.isEmpty() && tool.has("outputSchemaSha256")) {
                throw new IllegalArgumentException("output schema hash without output schema");
            }
            McpImportedTool pin = new McpImportedTool(
                required(tool, "mcpName"), required(tool, "operation"),
                ConnectorOperationKind.of(required(tool, "kind")), tool.get("majorVersion").intValue(),
                required(tool, "input"), required(tool, "output"), input, output,
                required(tool, "projectionId"), McpImportedTool.ResultMode.from(required(tool, "resultMode")));
            if (!pin.sha256().equals(required(tool, "pinSha256"))) {
                throw new IllegalArgumentException("MCP projection/selection pin hash mismatch");
            }
            result.add(pin);
        }
        return new McpImportedToolCatalog(result);
    }

    private static McpJsonSchema schema(JsonNode tool, String field, List<String> selection) {
        McpJsonSchema schema = new McpJsonSchema(tool.path(field).toString(), selection);
        if (!schema.sha256().equals(required(tool, field + "Sha256"))) {
            throw new IllegalArgumentException("MCP " + field + " hash mismatch");
        }
        return schema;
    }

    private static void fields(JsonNode node, Set<String> allowed) {
        if (!node.isObject()) {
            throw new IllegalArgumentException("MCP pin must be an object");
        }
        node.fieldNames().forEachRemaining(field -> {
            if (!allowed.contains(field)) {
                throw new IllegalArgumentException("unsupported MCP pin field '" + field + "'");
            }
        });
    }

    private static String required(JsonNode value, String field) {
        JsonNode result = value.path(field);
        if (!result.isTextual() || result.textValue().isBlank()) {
            throw new IllegalArgumentException("pinned MCP tool field '" + field + "' must be a non-blank string");
        }
        return result.textValue();
    }
}
