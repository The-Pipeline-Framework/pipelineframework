package org.pipelineframework.connector.mcp.maven;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.apache.maven.plugin.MojoExecutionException;
import org.pipelineframework.connector.ConnectorProviderManifestReader;
import org.pipelineframework.connector.mcp.McpImportedTool;
import org.pipelineframework.connector.mcp.McpImportedToolCatalog;
import io.modelcontextprotocol.spec.McpSchema;

class RefreshMcpImportMojoTest {
    @Test
    void importsOnlySelectedInvoiceFieldsAndRecordsSelectionDeterministically() throws Exception {
        Map<String, Object> input = Map.of("type", "object", "additionalProperties", false,
            "required", List.of("params"), "properties", Map.of("params", Map.of(
                "type", "object", "additionalProperties", false, "required", List.of("customer_id", "line_items"),
                "properties", Map.of("customer_id", Map.of("type", "string", "minLength", 1),
                    "line_items", Map.of("type", "array", "minItems", 1, "items", Map.of("type", "string")),
                    "memo", Map.of("type", "string"),
                    "linked_txn", Map.of("type", "array", "items", Map.of("oneOf", List.of()))))));
        var tool = io.modelcontextprotocol.spec.McpSchema.Tool.builder("create_invoice").inputSchema(input)
            .outputSchema(Map.of("type", "object", "additionalProperties", false, "properties", Map.of())).build();
        McpToolMapping mapping = new McpToolMapping();
        mapping.mcpName = "create_invoice";
        mapping.operation = "invoice.create";
        mapping.kind = "command";
        mapping.majorVersion = 1;
        mapping.inputType = "CreateInvoice";
        mapping.outputType = "InvoiceCreated";
        assertThrows(IllegalArgumentException.class, () -> RefreshMcpImportMojo.importTools(List.of(tool), List.of(mapping)));
        mapping.includeFields = List.of("params.line_items", "params.customer_id");
        var first = RefreshMcpImportMojo.importTools(List.of(tool, McpImportStdioServerMain.tool("discovered-only")),
            List.of(mapping));
        RefreshMcpImportMojo.write(temporary, first);
        String firstPin = Files.readString(pin());
        String firstManifest = Files.readString(manifest());
        assertTrue(firstPin.contains("includeFields"));
        assertFalse(firstManifest.contains("linked_txn"));
        assertFalse(firstPin.contains("discovered-only"));
        assertTrue(firstManifest.contains("minItems"));
        mapping.includeFields = List.of("params.customer_id", "params.line_items");
        RefreshMcpImportMojo.write(temporary, RefreshMcpImportMojo.importTools(List.of(tool), List.of(mapping)));
        assertEquals(firstPin, Files.readString(pin()));
        assertEquals(firstManifest, Files.readString(manifest()));
        assertEquals(mapping.includeFields, JSON.convertValue(
            JSON.readTree(firstPin).path("tools").get(0).path("includeFields"), List.class));
        mapping.includeFields = List.of("params.customer_id", "params.line_items", "params.memo");
        RefreshMcpImportMojo.write(temporary, RefreshMcpImportMojo.importTools(List.of(tool), List.of(mapping)));
        assertFalse(firstPin.equals(Files.readString(pin())));
        assertTrue(Files.readString(manifest()).contains("memo"));
        var unstructured = McpSchema.Tool.builder("create_invoice").inputSchema(input).build();
        RefreshMcpImportMojo.write(temporary, RefreshMcpImportMojo.importTools(List.of(unstructured), List.of(mapping)));
        var restored = McpImportedToolCatalog.read(Files.readString(pin())).tools().getFirst();
        assertEquals(McpImportedTool.ResultMode.JSON_PAYLOAD, restored.resultMode());
        assertEquals(mapping.includeFields, restored.inputSchema().includeFields());
        assertTrue(restored.inputSchema().json().contains("linked_txn"));
        restored.inputSchema().validateArguments(JSON.readTree(
            "{\"params\":{\"customer_id\":\"42\",\"line_items\":[\"item\"]}}"));
        assertThrows(IllegalArgumentException.class, () -> restored.inputSchema().validateArguments(JSON.readTree(
            "{\"params\":{\"customer_id\":\"42\",\"line_items\":[\"item\"],\"linked_txn\":[]}}")));
    }

    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir
    Path temporary;

    @Test
    void importsUnstructuredToolWithCanonicalPayloadAndRoundTripsPrivateSchemaPin() throws Exception {
        McpToolMapping mapping = mapping();
        var schema = Map.<String, Object>of("type", "object", "additionalProperties", false,
            "properties", Map.of("id", Map.of("type", "string")), "required", List.of("id"));
        var selected = McpSchema.Tool.builder("selected").inputSchema(schema).build();
        var discoveredOnly = McpSchema.Tool.builder("unused").inputSchema(schema).build();
        var imported = RefreshMcpImportMojo.importTools(List.of(discoveredOnly, selected), List.of(mapping));
        RefreshMcpImportMojo.write(temporary, imported);
        var contract = imported.operations().getFirst().typeContract().orElseThrow();
        assertEquals(McpImportedTool.JSON_PAYLOAD, contract.outputType().orElseThrow());
        assertEquals(List.of("SelectedRequest"), imported.types().stream().map(t -> t.identity().typeName()).toList());
        var pins = McpImportedToolCatalog.read(Files.readString(pin()));
        assertEquals(imported.pins(), pins.tools());
        assertEquals(McpImportedTool.ResultMode.JSON_PAYLOAD, pins.tools().getFirst().resultMode());
        assertTrue(pins.tools().getFirst().outputSchema().isEmpty());
        assertFalse(Files.readString(pin()).contains("unused"));
        assertFalse(Files.readString(manifest()).contains("bodyJson")); // supplied by tpf.connector, not MCP public metadata
    }

    @Test
    void objectKeyReorderingKeepsImportByteIdenticalAndUnselectedSchemasDoNotAffectPins() throws Exception {
        McpToolMapping mapping = mapping();
        mapping.outputType = "SelectedResult";
        var schema = new java.util.LinkedHashMap<String, Object>();
        schema.put("type", "object");
        schema.put("additionalProperties", false);
        schema.put("properties", Map.of("id", Map.of("type", "string")));
        schema.put("required", List.of("id"));
        var reverse = new java.util.LinkedHashMap<String, Object>();
        new java.util.ArrayList<>(schema.keySet()).reversed().forEach(key -> reverse.put(key, schema.get(key)));
        var first = RefreshMcpImportMojo.importTools(List.of(
            McpSchema.Tool.builder("selected").inputSchema(schema).outputSchema(schema).build()), List.of(mapping));
        var second = RefreshMcpImportMojo.importTools(List.of(
            McpSchema.Tool.builder("selected").inputSchema(reverse).outputSchema(reverse).build(),
            McpSchema.Tool.builder("unused").inputSchema(Map.of("$ref", "https://unused.invalid")).build()), List.of(mapping));
        RefreshMcpImportMojo.write(temporary, first);
        String pinned = Files.readString(pin());
        String publicContract = Files.readString(manifest());
        RefreshMcpImportMojo.write(temporary, second);
        assertEquals(pinned, Files.readString(pin()));
        assertEquals(publicContract, Files.readString(manifest()));
        assertEquals(first.pins(), McpImportedToolCatalog.read(pinned).tools());
    }

    @Test
    void invalidDeclaredOutputNeverSelectsEnvelope() {
        var mapping = mapping();
        mapping.outputType = "SelectedResult";
        assertThrows(IllegalArgumentException.class, () -> RefreshMcpImportMojo.importTools(List.of(
            McpSchema.Tool.builder("selected").inputSchema(Map.of("type", "object", "additionalProperties", false))
                .outputSchema(Map.of("type", "string")).build()), List.of(mapping)));
    }

    private McpToolMapping mapping() {
        var mapping = new McpToolMapping();
        mapping.mcpName = "selected";
        mapping.operation = "read.selected";
        mapping.kind = "query";
        mapping.majorVersion = 1;
        mapping.inputType = "SelectedRequest";
        return mapping;
    }

    @Test
    void refreshesFromARealOfficialSdkStdioServerAndOmitsDiscoveredOnlyTools() throws Exception {
        RefreshMcpImportMojo mojo = mojo("stdio");
        set(mojo, "command", Path.of(System.getProperty("java.home"), "bin", "java").toString());
        set(mojo, "arguments", List.of(
            "-cp", System.getProperty("java.class.path"), McpImportStdioServerMain.class.getName()));

        mojo.execute();

        assertPinnedImport();
    }

    @Test
    void refreshesOverStreamableHttpWithoutPersistingEndpoint() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/mcp", RefreshMcpImportMojoTest::handleMcp);
        server.start();
        try {
            RefreshMcpImportMojo mojo = mojo("streamable-http");
            String endpoint = "http://127.0.0.1:" + server.getAddress().getPort() + "/mcp";
            set(mojo, "endpoint", endpoint);

            mojo.execute();

            assertPinnedImport();
            assertFalse(Files.readString(pin()).contains(endpoint));
            assertFalse(Files.readString(manifest()).contains(endpoint));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void rejectsRelativeStreamableHttpEndpointBeforeConnecting() throws Exception {
        RefreshMcpImportMojo mojo = mojo("streamable-http");
        set(mojo, "endpoint", "/mcp");

        MojoExecutionException failure = assertThrows(MojoExecutionException.class, mojo::execute);

        assertTrue(failure.getMessage().contains("absolute HTTP(S) URI"));
    }

    @Test
    void rejectsHostHeadersOverPlainHttpBeforeConnecting() throws Exception {
        RefreshMcpImportMojo mojo = mojo("streamable-http");
        set(mojo, "endpoint", "http://127.0.0.1:1/mcp");
        set(mojo, "headers", Map.of("Authorization", "PATH"));

        MojoExecutionException failure = assertThrows(MojoExecutionException.class, mojo::execute);

        assertTrue(failure.getMessage().contains("headers require an HTTPS endpoint"));
    }

    private RefreshMcpImportMojo mojo(String transport) throws Exception {
        McpToolMapping mapping = new McpToolMapping();
        mapping.mcpName = "selected";
        mapping.operation = "read.selected";
        mapping.kind = "query";
        mapping.majorVersion = 1;
        mapping.inputType = "SelectedRequest";
        mapping.outputType = "SelectedResult";
        RefreshMcpImportMojo mojo = new RefreshMcpImportMojo();
        set(mojo, "outputDirectory", temporary.toFile());
        set(mojo, "transport", transport);
        set(mojo, "timeout", "PT10S");
        set(mojo, "tools", List.of(mapping));
        return mojo;
    }

    private void assertPinnedImport() throws Exception {
        String pin = Files.readString(pin());
        assertTrue(pin.contains("\"mcpName\":\"selected\""));
        assertFalse(pin.contains("discovered-only"));
        var imported = ConnectorProviderManifestReader.read(Files.newInputStream(manifest()))
            .providers().stream().filter(provider -> provider.provider().id().value().equals("mcp.client"))
            .findFirst().orElseThrow();
        assertEquals(List.of("read.selected"), imported.operations().stream().map(operation -> operation.id()).toList());
        assertEquals(List.of("SelectedRequest", "SelectedResult"), imported.protocolTypes().stream()
            .map(type -> type.identity().typeName()).sorted().toList());
    }

    private Path manifest() {
        return temporary.resolve("META-INF/pipeline/connector-providers.json");
    }

    private Path pin() {
        return temporary.resolve("META-INF/pipeline/mcp-tools.json");
    }

    private static void set(Object target, String name, Object value) throws Exception {
        Field field = RefreshMcpImportMojo.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static void handleMcp(HttpExchange exchange) throws IOException {
        try (exchange) {
            if ("DELETE".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(200, -1);
                return;
            }
            JsonNode request = JSON.readTree(exchange.getRequestBody());
            if (!request.has("id")) {
                exchange.sendResponseHeaders(202, -1);
                return;
            }
            String method = request.path("method").asText();
            Map<String, Object> result;
            if ("initialize".equals(method)) {
                String protocol = request.path("params").path("protocolVersion").asText();
                result = Map.of(
                    "protocolVersion", protocol,
                    "capabilities", Map.of("tools", Map.of()),
                    "serverInfo", Map.of("name", "http-import-test", "version", "1"));
            } else if ("tools/list".equals(method)) {
                result = Map.of("tools", List.of(McpImportStdioServerMain.tool("selected")));
            } else {
                throw new IOException("unexpected MCP method " + method);
            }
            byte[] response = JSON.writeValueAsBytes(Map.of(
                "jsonrpc", "2.0", "id", JSON.treeToValue(request.get("id"), Object.class), "result", result));
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
        }
    }
}
