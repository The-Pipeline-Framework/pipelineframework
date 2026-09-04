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

class RefreshMcpImportMojoTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir
    Path temporary;

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
