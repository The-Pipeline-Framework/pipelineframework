package org.pipelineframework.type;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CanonicalTypeCatalogueRepeatedFieldTest {

    private final CanonicalTypeCatalogue catalogue = CanonicalTypeCatalogue.fromCanonicalTypes(Map.of(
        "LineItem", binding("LineItem", List.of(field("sku", scalar("string"), false))),
        "Batch", binding("Batch", List.of(field("lineItems", named("LineItem"), true)))));

    @Test
    void projectsRepeatedFieldsAsOptionalDefaultEmptyArrays() throws Exception {
        JsonNode schema = PipelineJson.mapper().readTree(catalogue.schema("Batch"));

        JsonNode lineItems = schema.path("properties").path("lineItems");
        assertEquals("array", lineItems.path("type").textValue());
        assertEquals("#/$defs/LineItem", lineItems.path("items").path("$ref").textValue());
        assertTrue(lineItems.path("default").isArray());
        assertEquals(0, schema.path("required").size());
    }

    @Test
    void missingRepeatedFieldCanonicalizesToEmptyForAdditiveJsonCompatibility() {
        assertEquals("{\"lineItems\":[]}", catalogue.validateAndCanonicalize("Batch", "{}"));
    }

    @Test
    void validatesEachElementAndPreservesArrayOrderAndDuplicates() {
        String payload = """
            {"lineItems":[{"sku":"b"},{"sku":"a"},{"sku":"a"}]}
            """;

        assertEquals("{\"lineItems\":[{\"sku\":\"b\"},{\"sku\":\"a\"},{\"sku\":\"a\"}]}",
            catalogue.validateAndCanonicalize("Batch", payload));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> catalogue.validateAndCanonicalize("Batch", "{\"lineItems\":[{\"sku\":\"ok\"},{\"sku\":7}]}"));
        assertTrue(error.getMessage().contains("$.lineItems[1].sku"), error.getMessage());
    }

    private static Map<String, Object> binding(String name, List<Map<String, Object>> fields) {
        return Map.of("definition", Map.of("id", name, "kind", "record", "fields", fields));
    }

    private static Map<String, Object> field(String name, Map<String, Object> type, boolean repeated) {
        return repeated
            ? Map.of("name", name, "type", type, "repeated", true)
            : Map.of("name", name, "type", type);
    }

    private static Map<String, Object> scalar(String id) {
        return Map.of("kind", "scalar", "id", id);
    }

    private static Map<String, Object> named(String id) {
        return Map.of("kind", "named", "id", id);
    }
}
