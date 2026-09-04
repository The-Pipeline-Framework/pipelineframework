package org.pipelineframework.connector.mcp.maven;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;

/** Official-SDK MCP server process used by refresh integration coverage. */
public final class McpImportStdioServerMain {
    private McpImportStdioServerMain() {
    }

    public static void main(String[] arguments) throws InterruptedException {
        var transport = new StdioServerTransportProvider(
            new JacksonMcpJsonMapper(new ObjectMapper()));
        McpServer.sync(transport)
            .serverInfo("tpf-import-test", "1")
            .toolCall(tool("selected"), (exchange, request) -> McpSchema.CallToolResult.builder()
                .structuredContent(Map.of("value", "ok"))
                .content(List.of())
                .isError(false)
                .build())
            .toolCall(tool("discovered-only"), (exchange, request) -> McpSchema.CallToolResult.builder()
                .structuredContent(Map.of("value", "not-imported"))
                .content(List.of())
                .isError(false)
                .build())
            .build();
        Thread.currentThread().join();
    }

    static McpSchema.Tool tool(String name) {
        Map<String, Object> input = Map.of(
            "type", "object", "additionalProperties", false,
            "required", List.of("id"), "properties", Map.of("id", Map.of("type", "string")));
        Map<String, Object> output = Map.of(
            "type", "object", "additionalProperties", false,
            "required", List.of("value"), "properties", Map.of("value", Map.of("type", "string")));
        return McpSchema.Tool.builder(name).inputSchema(input).outputSchema(output).build();
    }
}
