package org.pipelineframework.host.quickbooks;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;

/** Real protocol subprocess that owns opaque private state, without external API calls. */
public final class QuickBooksFixture {
    private QuickBooksFixture() { }

    public static void main(String[] arguments) throws Exception {
        Path file = Path.of("").toAbsolutePath().resolve("node-private-state");
        Files.writeString(file.resolveSibling("starts"), "started\n", java.nio.file.StandardOpenOption.CREATE,
            java.nio.file.StandardOpenOption.APPEND);
        if (arguments.length > 0 && arguments[0].equals("fail")) {
            throw new IllegalStateException("Simulated initialization failure");
        }
        if (arguments.length > 0 && arguments[0].equals("stubborn")) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                while (!Files.exists(file.resolveSibling("allow-exit"))) {
                    try {
                        Thread.sleep(25);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }));
        }
        String content = Files.exists(file) ? Files.readString(file) : "test-original";
        boolean restarted = content.contains("test-rotated");
        Files.writeString(file, content.replace("test-original", "test-rotated"));
        if (System.getenv("QUICKBOOKS_REFRESH_TOKEN") != null || System.getenv("NODE_OPTIONS") != null) {
            throw new IllegalStateException("Unexpected inherited credential or process settings");
        }
        String instance = file.getParent().getFileName().toString();
        var mapper = new JacksonMcpJsonMapper(new ObjectMapper());
        var transport = new StdioServerTransportProvider(mapper);
        var tool = McpSchema.Tool.builder("search_customers")
            .inputSchema(Map.of("type", "object", "properties", Map.of(), "additionalProperties", false))
            .build();
        McpServer.sync(transport).serverInfo("tpf-quickbooks-fixture", "1")
            .toolCall(tool, (exchange, request) -> McpSchema.CallToolResult.builder()
                .structuredContent(Map.of("instance", instance, "restarted", restarted))
                .content(List.of()).isError(false).build()).build();
        Thread.currentThread().join();
    }
}
