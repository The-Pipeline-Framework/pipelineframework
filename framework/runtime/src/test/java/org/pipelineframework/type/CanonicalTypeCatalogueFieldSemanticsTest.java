package org.pipelineframework.type;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;

import static org.junit.jupiter.api.Assertions.*;

class CanonicalTypeCatalogueFieldSemanticsTest {

    private final CanonicalTypeCatalogue catalogue = CanonicalTypeCatalogue.fromCanonicalTypes(Map.of(
        "Customer", Map.of("definition", Map.of("id", "Customer", "kind", "record", "fields", List.of(
            field("name", "REQUIRED", "NON_NULL"),
            field("nickname", "OPTIONAL", "NON_NULL"),
            field("middleName", "REQUIRED", "NULLABLE"),
            field("note", "OPTIONAL", "NULLABLE"))))));

    @Test
    void validatesAbsentNullAndPresentIndependently() {
        // required/non-null
        assertThrows(IllegalArgumentException.class,
            () -> catalogue.validateAndCanonicalize("Customer", "{\"middleName\":null}"));
        assertThrows(IllegalArgumentException.class,
            () -> catalogue.validateAndCanonicalize("Customer", "{\"name\":null,\"middleName\":null}"));
        assertEquals("{\"middleName\":null,\"name\":\"Ada\"}",
            catalogue.validateAndCanonicalize("Customer", "{\"name\":\"Ada\",\"middleName\":null}"));

        // required/nullable
        assertThrows(IllegalArgumentException.class,
            () -> catalogue.validateAndCanonicalize("Customer", "{\"name\":\"Ada\"}"));
        assertEquals("{\"middleName\":null,\"name\":\"Ada\"}",
            catalogue.validateAndCanonicalize("Customer", "{\"name\":\"Ada\",\"middleName\":null}"));
        assertEquals("{\"middleName\":\"L\",\"name\":\"Ada\"}",
            catalogue.validateAndCanonicalize("Customer", "{\"name\":\"Ada\",\"middleName\":\"L\"}"));

        // optional/non-null
        assertEquals("{\"middleName\":\"L\",\"name\":\"Ada\"}",
            catalogue.validateAndCanonicalize("Customer", "{\"name\":\"Ada\",\"middleName\":\"L\"}"));
        assertThrows(IllegalArgumentException.class,
            () -> catalogue.validateAndCanonicalize("Customer",
                "{\"name\":\"Ada\",\"nickname\":null,\"middleName\":\"L\"}"));
        assertEquals("{\"middleName\":\"L\",\"name\":\"Ada\",\"nickname\":\"A\"}",
            catalogue.validateAndCanonicalize("Customer",
                "{\"name\":\"Ada\",\"nickname\":\"A\",\"middleName\":\"L\"}"));

        // optional/nullable
        assertEquals("{\"middleName\":\"L\",\"name\":\"Ada\",\"note\":null}",
            catalogue.validateAndCanonicalize("Customer",
                "{\"name\":\"Ada\",\"middleName\":\"L\",\"note\":null}"));
        assertEquals("{\"middleName\":\"L\",\"name\":\"Ada\",\"note\":\"memo\"}",
            catalogue.validateAndCanonicalize("Customer",
                "{\"name\":\"Ada\",\"middleName\":\"L\",\"note\":\"memo\"}"));
    }

    @Test
    void schemaMapsTheFourStates() throws Exception {
        JsonNode schema = PipelineJson.mapper().readTree(catalogue.schema("Customer"));
        assertEquals(List.of("middleName", "name"),
            java.util.stream.StreamSupport.stream(schema.path("required").spliterator(), false)
                .map(JsonNode::textValue).sorted().toList());
        assertEquals("string", schema.path("properties").path("name").path("type").textValue());
        assertEquals("null", schema.path("properties").path("middleName").path("oneOf").get(1).path("type").textValue());
        assertEquals("null", schema.path("properties").path("note").path("oneOf").get(1).path("type").textValue());
    }

    private static Map<String, Object> field(String name, String presence, String nullability) {
        return Map.of("name", name, "type", Map.of("kind", "scalar", "id", "string"),
            "presence", presence, "nullability", nullability);
    }
}
