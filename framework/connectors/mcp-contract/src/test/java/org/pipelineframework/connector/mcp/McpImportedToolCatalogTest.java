package org.pipelineframework.connector.mcp;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.connector.ConnectorOperationKind;

import static org.junit.jupiter.api.Assertions.*;

class McpImportedToolCatalogTest {
    @TempDir
    Path temporary;

    private static final String SCHEMA = """
        {"type":"object","additionalProperties":false,"required":["id"],
         "properties":{"id":{"type":"string","minLength":1}}}
        """;

    @Test
    void hashesCanonicalSchemasAndTheWholeProjectionSelection() {
        var first = new McpJsonSchema(SCHEMA);
        var reordered = new McpJsonSchema("""
            {"properties":{"id":{"minLength":1.0,"type":"string"}},
             "required":["id"],"additionalProperties":false,"type":"object"}
            """);
        assertEquals(first, reordered);
        assertEquals(first.sha256(), reordered.sha256());
        assertNotEquals(first.sha256(), new McpJsonSchema(SCHEMA.replace("minLength\":1", "minLength\":2")).sha256());
        var pin = pin("read.one", first);
        assertNotEquals(pin.sha256(), pin("read.two", first).sha256());
        assertEquals(pin, McpImportedToolCatalog.read(new McpImportedToolCatalog(List.of(pin)).json()).tools().getFirst());
        String encoded = new McpImportedToolCatalog(List.of(pin)).json();
        assertThrows(IllegalArgumentException.class, () -> McpImportedToolCatalog.read(
            encoded.replace("read.one", "read.two")));
        assertThrows(IllegalArgumentException.class, () -> McpImportedToolCatalog.read(
            encoded.replace("minLength\":1", "minLength\":2")));
        assertThrows(IllegalArgumentException.class, () -> McpImportedToolCatalog.read(
            encoded.replace("tpf-mcp-importer-v1", "unknown-projection")));
    }

    @Test
    void pinsOriginalSchemaAndNarrowedSelectionWhileRejectingOmittedArguments() {
        String original = """
            {"type":"object","additionalProperties":false,"required":["params"],"properties":{
             "params":{"type":"object","additionalProperties":false,"required":["id"],"properties":{
              "id":{"type":"string","minLength":1},"memo":{"type":"string"},
              "unsupported":{"oneOf":[]}}}}}
            """;
        var selected = new McpJsonSchema(original, List.of("params.id"));
        var tool = pin("read.one", selected);
        String json = new McpImportedToolCatalog(List.of(tool)).json();
        var restored = McpImportedToolCatalog.read(json).tools().getFirst();
        assertEquals(tool, restored);
        assertTrue(restored.inputSchema().json().contains("oneOf"));
        restored.inputSchema().validateArguments(McpPinnedJson.parse("{\"params\":{\"id\":\"42\"}}"));
        for (String invalid : List.of("{}", "{\"params\":{}}", "{\"params\":{\"id\":\"\"}}",
            "{\"params\":{\"id\":\"42\",\"memo\":\"not selected\"}}",
            "{\"params\":{\"id\":\"42\",\"unsupported\":null}}")) {
            assertThrows(IllegalArgumentException.class, () -> restored.inputSchema()
                .validateArguments(McpPinnedJson.parse(invalid)));
        }
        var wider = pin("read.one", new McpJsonSchema(original, List.of("params.id", "params.memo")));
        assertEquals(tool.inputSchema().sha256(), wider.inputSchema().sha256());
        assertNotEquals(tool.sha256(), wider.sha256());
        assertNotEquals(tool.sha256(), pin("read.one", new McpJsonSchema(
            original.replace("oneOf", "anyOf"), List.of("params.id"))).sha256());
        assertThrows(IllegalArgumentException.class, () -> McpImportedToolCatalog.read(
            json.replace("[\"params.id\"]", "[\"params.id\",\"params.memo\"]")));
        assertThrows(IllegalArgumentException.class, () -> new McpJsonSchema(original,
            List.of("params.id", "params.unsupported")));
    }

    @Test
    void preservesArrayOrderAndNormalizesNumericValues() {
        assertEquals(McpPinnedJson.canonicalize(McpPinnedJson.parse("[1,1.0,1e0]")), "[1,1,1]");
        assertNotEquals(McpPinnedJson.sha256("[1,2]"), McpPinnedJson.sha256("[2,1]"));
        String expanded = McpPinnedJson.canonicalize(McpPinnedJson.parse("1e300"));
        assertEquals(expanded, McpPinnedJson.canonicalize(McpPinnedJson.parse(expanded)));
        assertThrows(IllegalArgumentException.class, () -> McpPinnedJson.parse("1e1000000000"));
    }

    @Test
    void resultModeAndOptionalSchemaMustAgree() {
        var pin = pin("read.one", new McpJsonSchema(SCHEMA));
        assertEquals(Optional.empty(), pin.outputSchema());
        assertFalse(pin.toJson().has("outputSchema"));
        assertThrows(IllegalArgumentException.class, () -> new McpImportedTool(
            "one", "read.one", ConnectorOperationKind.QUERY, 1, "Request", McpImportedTool.JSON_PAYLOAD,
            pin.inputSchema(), Optional.of(pin.inputSchema()), McpImportedTool.PROJECTION_V1,
            McpImportedTool.ResultMode.JSON_PAYLOAD));
        assertThrows(IllegalArgumentException.class, () -> McpImportedToolCatalog.read(
            new McpImportedToolCatalog(List.of(pin)).json().replace("json-payload", "structured")));
    }

    @Test
    void strictReaderRejectsUnknownFieldsDuplicateKeysLegacyVersionsAndTrailingDocuments() {
        String json = new McpImportedToolCatalog(List.of(pin("read.one", new McpJsonSchema(SCHEMA)))).json();
        for (String invalid : List.of(json.replace("\"provider\":", "\"endpoint\":\"secret\",\"provider\":"),
            json.replace("\"provider\":", "\"provider\":\"mcp.client\",\"provider\":"),
            json.replace("\"schemaVersion\":2", "\"schemaVersion\":1"), json + "{}")) {
            assertThrows(IllegalArgumentException.class, () -> McpImportedToolCatalog.read(invalid));
        }
    }

    @Test
    void classpathIdenticalPinsCoalesceAndConflictingPinsFailInEitherOrder() throws Exception {
        var first = pin("read.one", new McpJsonSchema(SCHEMA));
        Path a = resource("a", first);
        Path b = resource("b", first);
        try (var loader = new URLClassLoader(new URL[] {a.toUri().toURL(), b.toUri().toURL()},
            ClassLoader.getPlatformClassLoader())) {
            assertEquals(List.of(first), McpImportedToolCatalog.load(loader).tools());
        }
        resource("b", pin("read.one", new McpJsonSchema(SCHEMA.replace("minLength\":1", "minLength\":2"))));
        for (URL[] urls : List.of(new URL[] {a.toUri().toURL(), b.toUri().toURL()},
            new URL[] {b.toUri().toURL(), a.toUri().toURL()})) {
            try (var loader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader())) {
                assertTrue(assertThrows(IllegalArgumentException.class, () -> McpImportedToolCatalog.load(loader))
                    .getMessage().contains("conflicting pinned MCP operation identity"));
            }
        }
    }

    @Test
    void boundsResourcesAndSchemasWithoutResolvingReferences() {
        assertThrows(IllegalArgumentException.class, () -> new McpJsonSchema("""
            {"type":"object","$ref":"https://never-contact.invalid/schema"}
            """));
        assertThrows(IllegalArgumentException.class, () -> McpPinnedJson.parse("[".repeat(66) + "0" + "]".repeat(66)));
        assertThrows(IllegalArgumentException.class, () -> McpPinnedJson.parse("\"" + "a".repeat(1_048_577) + "\""));
        assertThrows(IllegalArgumentException.class, () -> McpPinnedJson.parse("[" + "0,".repeat(20_001) + "0]"));
    }

    private Path resource(String folder, McpImportedTool tool) throws Exception {
        Path root = temporary.resolve(folder);
        Path path = root.resolve(McpImportedToolCatalog.RESOURCE_PATH);
        Files.createDirectories(path.getParent());
        Files.writeString(path, new McpImportedToolCatalog(List.of(tool)).json());
        return root;
    }

    private McpImportedTool pin(String operation, McpJsonSchema input) {
        return new McpImportedTool("one", operation, ConnectorOperationKind.QUERY, 1, "Request",
            McpImportedTool.JSON_PAYLOAD, input, Optional.empty(), McpImportedTool.PROJECTION_V1,
            McpImportedTool.ResultMode.JSON_PAYLOAD);
    }
}
