package org.pipelineframework.connector.mcp;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;

/** Real MCP SDK server process used by the STDIO integration test. */
public final class McpStdioFixtureMain {
    private McpStdioFixtureMain() {
    }

    public static void main(String[] arguments) throws InterruptedException {
        // A test fixture must not outlive its Surefire host if the transport closes asynchronously.
        ProcessHandle.current().parent().ifPresent(parent -> parent.onExit().thenRun(() -> System.exit(0)));
        var mapper = new JacksonMcpJsonMapper(new ObjectMapper());
        var transport = new StdioServerTransportProvider(mapper);
        Map<String, Object> input = Map.of(
            "type", "object", "additionalProperties", false, "required", List.of("id"),
            "properties", Map.of("id", Map.of("type", "string")));
        Map<String, Object> output = Map.of(
            "type", "object", "additionalProperties", false, "required", List.of("value"),
            "properties", Map.of("value", Map.of("type", "string")));
        McpSchema.Tool tool = McpSchema.Tool.builder("lookup")
            .inputSchema(input)
            .outputSchema(output)
            .build();
        McpServer.sync(transport)
            .serverInfo("tpf-test-mcp", "1")
            .toolCall(McpSchema.Tool.builder("notes").inputSchema(input).build(),
                (exchange, request) -> McpSchema.CallToolResult.builder()
                    .addTextContent("notes-" + request.arguments().get("id")).isError(false).build())
            .toolCall(tool, (exchange, request) -> McpSchema.CallToolResult.builder()
                .structuredContent(Map.of("value", "real-" + request.arguments().get("id")))
                .content(List.of())
                .isError(false)
                .build())
            .build();
        Thread.currentThread().join();
    }
}
