package org.pipelineframework.connector.http;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class HttpWireValueValidatorTest {
    private static final HttpWireSchema SCHEMA = new HttpWireSchema("""
        {
          "type":"object",
          "additionalProperties":false,
          "required":["kind","items"],
          "properties":{
            "kind":{"type":"string","enum":["alpha","beta"]},
            "items":{"type":"array","minItems":1,"maxItems":2,
              "items":{"type":"integer","minimum":1}}
          }
        }
        """);

    @Test
    void validatesTheBoundedRuntimeSchemaSubset() {
        assertDoesNotThrow(() -> HttpWireValueValidator.validate(
            HttpPinnedJson.parse("{\"kind\":\"alpha\",\"items\":[1,2]}"), SCHEMA));
        assertThrows(IllegalArgumentException.class, () -> HttpWireValueValidator.validate(
            HttpPinnedJson.parse("{\"kind\":\"other\",\"items\":[1]}"), SCHEMA));
        assertThrows(IllegalArgumentException.class, () -> HttpWireValueValidator.validate(
            HttpPinnedJson.parse("{\"kind\":\"alpha\",\"items\":[0]}"), SCHEMA));
        assertThrows(IllegalArgumentException.class, () -> HttpWireValueValidator.validate(
            HttpPinnedJson.parse("{\"kind\":\"alpha\",\"items\":[1],\"extra\":true}"), SCHEMA));
    }

    @Test
    void rejectsAmbiguousOneOfMatches() {
        HttpWireSchema ambiguous = new HttpWireSchema("""
            {"oneOf":[{"type":"number"},{"type":"integer"}]}
            """);

        assertThrows(IllegalArgumentException.class, () ->
            HttpWireValueValidator.validate(HttpPinnedJson.parse("1"), ambiguous));
    }
}
