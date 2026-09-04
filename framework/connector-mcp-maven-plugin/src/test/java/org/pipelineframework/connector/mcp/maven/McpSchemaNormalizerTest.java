package org.pipelineframework.connector.mcp.maven;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.modelcontextprotocol.spec.McpSchema;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.config.template.PipelineFieldNullability;
import org.pipelineframework.config.template.PipelineFieldPresence;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;

class McpSchemaNormalizerTest {
    @TempDir
    Path temporary;

    private final McpSchemaNormalizer normalizer = new McpSchemaNormalizer();

    @Test
    void mapsClosedObjectsIntoCanonicalV3WithoutInventingMcpTypes() {
        Map<String, Object> schema = Map.of(
            "type", "object",
            "additionalProperties", false,
            "required", List.of("count", "tags"),
            "properties", Map.of(
                "count", Map.of("type", "integer", "format", "int32", "minimum", 1),
                "note", Map.of("type", List.of("string", "null")),
                "tags", Map.of("type", "array", "items", Map.of("type", "string"))));

        var types = normalizer.normalize("ImportRequest", schema, "test input");
        PipelineTemplateTypeDefinition.RecordType root = assertInstanceOf(
            PipelineTemplateTypeDefinition.RecordType.class,
            types.stream().filter(type -> type.identity().typeName().equals("ImportRequest"))
                .findFirst().orElseThrow().definition());

        assertEquals(List.of("count", "note", "tags"), root.fields().stream().map(
            PipelineTemplateTypeDefinition.Field::name).toList());
        assertEquals(PipelineFieldPresence.OPTIONAL, root.fields().get(1).presence());
        assertEquals(PipelineFieldNullability.NULLABLE, root.fields().get(1).nullability());
        assertTrue(root.fields().get(2).repeated());
        assertTrue(types.stream().anyMatch(type -> type.identity().typeName().equals("ImportRequestCountValue")));
    }

    @Test
    void mapsAClosedZeroArgumentSchemaWithoutProperties() {
        var types = normalizer.normalize(
            "NoArguments", Map.of("type", "object", "additionalProperties", false), "tool input");

        PipelineTemplateTypeDefinition.RecordType root = assertInstanceOf(
            PipelineTemplateTypeDefinition.RecordType.class, types.getFirst().definition());
        assertEquals("NoArguments", root.name());
        assertTrue(root.fields().isEmpty());
    }

    @Test
    void rejectsNullableRootObjectSchema() {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
            () -> normalizer.normalize(
                "Request",
                Map.of("type", List.of("object", "null"), "additionalProperties", false),
                "tool input"));

        assertTrue(failure.getMessage().contains("root schema must have non-null type object"));
    }

    @Test
    void rejectsLossyLengthBoundsAndUnsupportedNumericFormats() {
        for (Number invalid : List.of(1.5d, Long.MAX_VALUE)) {
            IllegalArgumentException length = assertThrows(IllegalArgumentException.class,
                () -> normalizer.normalize(
                    "Request", Map.of(
                        "type", "object", "additionalProperties", false, "required", List.of("value"),
                        "properties", Map.of("value", Map.of(
                            "type", "string", "minLength", invalid))),
                    "tool input"));
            assertTrue(length.getMessage().contains("$.properties.value.minLength"));
        }

        for (Map<String, Object> unsupported : List.<Map<String, Object>>of(
            Map.of("type", "integer", "format", "uint64"),
            Map.of("type", "number", "format", "float16"))) {
            IllegalArgumentException format = assertThrows(IllegalArgumentException.class,
                () -> normalizer.normalize(
                    "Request", Map.of(
                        "type", "object", "additionalProperties", false, "required", List.of("value"),
                        "properties", Map.of("value", unsupported)),
                    "tool input"));
            assertTrue(format.getMessage().contains("$.properties.value.format"));
        }
    }

    @Test
    void rejectsImporterV1LimitationsWithJsonPathDiagnostics() {
        IllegalArgumentException open = assertThrows(IllegalArgumentException.class, () -> normalizer.normalize(
            "Request", Map.of("type", "object", "properties", Map.of()), "tool input"));
        assertTrue(open.getMessage().contains("$.additionalProperties"));

        IllegalArgumentException optionalArray = assertThrows(IllegalArgumentException.class, () -> normalizer.normalize(
            "Request", Map.of(
                "type", "object", "additionalProperties", false,
                "properties", Map.of("values", Map.of(
                    "type", "array", "items", Map.of("type", "string")))), "tool input"));
        assertTrue(optionalArray.getMessage().contains("$.properties.values"));

        IllegalArgumentException reference = assertThrows(IllegalArgumentException.class, () -> normalizer.normalize(
            "Request", Map.of(
                "type", "object", "additionalProperties", false, "required", List.of("value"),
                "properties", Map.of("value", Map.of("$ref", "#/$defs/Value", "type", "string"))), "tool input"));
        assertTrue(reference.getMessage().contains("$.properties.value.$ref"));

        IllegalArgumentException arrayComposition = assertThrows(IllegalArgumentException.class,
            () -> normalizer.normalize(
                "Request", Map.of(
                    "type", "object", "additionalProperties", false, "required", List.of("values"),
                    "properties", Map.of("values", Map.of(
                        "type", "array", "items", Map.of("type", "string"),
                        "oneOf", List.of(Map.of("type", "array"))))), "tool input"));
        assertTrue(arrayComposition.getMessage().contains("$.properties.values.oneOf"));

        for (String keyword : List.of("minItems", "maxItems", "uniqueItems")) {
            IllegalArgumentException arrayConstraint = assertThrows(IllegalArgumentException.class,
                () -> normalizer.normalize(
                    "Request", Map.of(
                        "type", "object", "additionalProperties", false, "required", List.of("values"),
                        "properties", Map.of("values", Map.of(
                            "type", "array", "items", Map.of("type", "string"), keyword, 1))),
                    "tool input"));
            assertTrue(arrayConstraint.getMessage().contains("$.properties.values." + keyword));
        }
    }

    @Test
    void discoveryDoesNotImportOrExposeUnmappedTools() {
        Map<String, Object> object = Map.of(
            "type", "object", "additionalProperties", false, "properties", Map.of());
        McpSchema.Tool selected = new McpSchema.Tool(
            "selected", null, null, object, object, null, Map.of());
        McpSchema.Tool discoveredOnly = new McpSchema.Tool(
            "discovered-only", null, null, object, object, null, Map.of());
        McpToolMapping mapping = new McpToolMapping();
        mapping.mcpName = "selected";
        mapping.operation = "read.selected";
        mapping.kind = "query";
        mapping.majorVersion = 1;
        mapping.inputType = "SelectedRequest";
        mapping.outputType = "SelectedResult";

        RefreshMcpImportMojo.ImportedArtifacts imported = RefreshMcpImportMojo.importTools(
            List.of(discoveredOnly, selected), List.of(mapping));

        assertEquals(1, imported.operations().size());
        assertEquals("read.selected", imported.operations().getFirst().id());
        assertEquals(ConnectorOperationKind.QUERY, imported.operations().getFirst().kind());
        assertEquals("selected", imported.pins().getFirst().get("mcpName"));
    }

    @Test
    void writesDeterministicStandardAndPrivatePinsWithoutTransportSecrets() throws Exception {
        Map<String, Object> object = Map.of(
            "type", "object", "additionalProperties", false, "properties", Map.of());
        McpSchema.Tool selected = new McpSchema.Tool(
            "selected", "ignored title", "ignored description", object, object, null,
            Map.of("credential", "sentinel-secret"));
        McpToolMapping mapping = new McpToolMapping();
        mapping.mcpName = "selected";
        mapping.operation = "read.selected";
        mapping.kind = "query";
        mapping.majorVersion = 1;
        mapping.inputType = "SelectedRequest";
        mapping.outputType = "SelectedResult";
        RefreshMcpImportMojo.ImportedArtifacts imported = RefreshMcpImportMojo.importTools(
            List.of(selected), List.of(mapping));

        RefreshMcpImportMojo.write(temporary, imported);
        String manifest = Files.readString(temporary.resolve("META-INF/pipeline/connector-providers.json"));
        String pin = Files.readString(temporary.resolve("META-INF/pipeline/mcp-tools.json"));
        RefreshMcpImportMojo.write(temporary, imported);

        assertEquals(manifest, Files.readString(temporary.resolve("META-INF/pipeline/connector-providers.json")));
        assertEquals(pin, Files.readString(temporary.resolve("META-INF/pipeline/mcp-tools.json")));
        assertTrue(manifest.contains("\"id\":\"mcp.client\""));
        assertTrue(pin.contains("\"mcpName\":\"selected\""));
        assertTrue(!manifest.contains("sentinel-secret"));
        assertTrue(!pin.contains("sentinel-secret"));
        assertTrue(!manifest.contains("ignored description"));
    }
}
