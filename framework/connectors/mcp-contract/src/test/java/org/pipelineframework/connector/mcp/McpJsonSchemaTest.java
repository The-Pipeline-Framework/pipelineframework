package org.pipelineframework.connector.mcp;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class McpJsonSchemaTest {
    @Test
    void validatesFinalOutboundObjectIncludingNestedBoundsNullabilityAndNumericEquality() {
        var schema = new McpJsonSchema("""
            {"type":"object","additionalProperties":false,"required":["lines","mode","count"],
             "properties":{
              "lines":{"type":"array","minItems":1,"maxItems":2,"items":{
               "type":"object","required":["amount"],"additionalProperties":false,"properties":{
                "amount":{"type":"number","exclusiveMinimum":0,"maximum":100}}}},
              "mode":{"type":"string","enum":["invoice","credit"],"minLength":1,"maxLength":10},
              "count":{"type":"integer","const":1.0},
              "memo":{"type":["string","null"],"pattern":"ok"}}}
            """);
        String valid = """
            {"lines":[{"amount":20.5}],"mode":"invoice","count":1,"memo":null}
            """;
        schema.validateArguments(McpPinnedJson.parse(valid));
        schema.validateArguments(McpPinnedJson.parse(valid.replace("null", "\"looks ok!\"")));
        for (String invalid : java.util.List.of(
            valid.replace("[{\"amount\":20.5}]", "[]"), valid.replace("20.5", "0"),
            valid.replace("20.5", "101"), valid.replace("invoice", "other"), valid.replace("\"count\":1", "\"count\":2"),
            valid.replace("null", "\"bad\""), valid.replace("\"memo\"", "\"extra\""), "{}", "[]")) {
            assertThrows(IllegalArgumentException.class, () -> schema.validateArguments(McpPinnedJson.parse(invalid)), invalid);
        }
    }

    @Test
    void patternsCannotTriggerExponentialBacktrackingOrReferenceExpansion() {
        var schema = new McpJsonSchema("""
            {"type":"object","properties":{"text":{"type":"string","pattern":"^(a+)+$"}}}
            """);
        assertTimeoutPreemptively(java.time.Duration.ofSeconds(2), () ->
            assertThrows(IllegalArgumentException.class, () -> schema.validateArguments(
                McpPinnedJson.parse("{\"text\":\"" + "a".repeat(20_000) + "!\"}"))));
        assertThrows(IllegalArgumentException.class, () -> new McpJsonSchema("""
            {"type":"object","properties":{"text":{"type":"string","pattern":"(?=x)x"}}}
            """));
    }

    @Test
    void stringLengthUsesUnicodeCodePointsAndPatternUsesSearchSemantics() {
        var schema = new McpJsonSchema("""
            {"type":"object","properties":{"text":{"type":"string","minLength":1,"maxLength":1,"pattern":"😀"}}}
            """);
        schema.validateArguments(McpPinnedJson.parse("{\"text\":\"😀\"}"));
        assertThrows(IllegalArgumentException.class, () -> schema.validateArguments(McpPinnedJson.parse("{\"text\":\"😀😀\"}")));
    }
}
