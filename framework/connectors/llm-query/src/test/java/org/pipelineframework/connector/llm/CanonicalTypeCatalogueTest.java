package org.pipelineframework.connector.llm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.type.CanonicalTypeCatalogue;

class CanonicalTypeCatalogueTest {
    private final CanonicalTypeCatalogue catalogue =
        CanonicalTypeCatalogue.load(CanonicalTypeCatalogueTest.class.getClassLoader());

    @Test
    void projectsAnAliasThatResolvesToARecordAsAnObjectSchema() {
        String schema = catalogue.schema("ToolArgumentsAlias");

        assertTrue(schema.contains("\"type\":\"object\""));
        assertTrue(schema.contains("\"amount\""));
    }

    @Test
    void projectsTrustedTopLevelFieldsOutOfTheModelSchemaWithoutWeakeningCanonicalValidation() throws Exception {
        var schema = PipelineJson.mapper().readTree(catalogue.schema("ToolArguments", Set.of("note")));

        assertTrue(schema.path("properties").has("amount"));
        assertFalse(schema.path("properties").has("note"));
        assertFalse(schema.path("required").toString().contains("note"));
        assertThrows(IllegalArgumentException.class,
            () -> catalogue.validateAndCanonicalize("ToolArguments", "{\"amount\":42}"));
        assertDoesNotThrow(() -> catalogue.validateAndCanonicalize(
            "ToolArguments", "{\"amount\":42,\"note\":\"trusted\"}"));
    }

    @Test
    void projectsOnlyDefinitionsReachableFromTheRootType() throws Exception {
        var schema = PipelineJson.mapper().readTree(catalogue.schema("ConstraintArguments"));
        Set<String> definitions = new TreeSet<>();
        schema.path("$defs").fieldNames().forEachRemaining(definitions::add);

        assertEquals(Set.of("EmailAddress"), definitions);
        assertFalse(schema.toString().contains("ReviewReady"));
        assertFalse(schema.toString().contains("ToolArguments"));
    }

    @Test
    void enforcesInt32AndInt64Boundaries() {
        assertDoesNotThrow(() -> catalogue.validateAndCanonicalize("ConstraintArguments", """
            {"email":"a@example.test","large":9223372036854775807,"small":2147483647}
            """));
        assertThrows(IllegalArgumentException.class,
            () -> catalogue.validateAndCanonicalize("ConstraintArguments", """
                {"email":"a@example.test","large":0,"small":2147483648}
                """));
        assertThrows(IllegalArgumentException.class,
            () -> catalogue.validateAndCanonicalize("ConstraintArguments", """
                {"email":"a@example.test","large":9223372036854775808,"small":0}
                """));
    }

    @Test
    void validatesSupportedFormatsAndRejectsUnsupportedCanonicalFormats() {
        assertThrows(IllegalArgumentException.class,
            () -> catalogue.validateAndCanonicalize("ConstraintArguments", """
                {"email":"not-an-email","large":0,"small":0}
                """));
        assertThrows(IllegalStateException.class,
            () -> catalogue.validateAndCanonicalize("UnsupportedFormatArguments", "{\"value\":\"host\"}"));
    }

    @Test
    void boundsModelTextBeforeRegularExpressionMatching() {
        String oversized = "a".repeat(4_097);

        assertThrows(IllegalArgumentException.class,
            () -> catalogue.validateAndCanonicalize("PatternArguments", "{\"value\":\"" + oversized + "\"}"));
    }

    @Test
    void projectsAndEnforcesAllowedValuesAndRepeatedFieldBounds() throws Exception {
        var schema = PipelineJson.mapper().readTree(catalogue.schema("InvoiceArguments"));

        assertEquals(1, schema.path("properties").path("methods").path("minItems").intValue());
        assertEquals(2, schema.path("properties").path("methods").path("maxItems").intValue());
        assertEquals("Accrual", schema.path("$defs").path("AccountingMethod").path("enum").get(0).textValue());
        assertDoesNotThrow(() -> catalogue.validateAndCanonicalize(
            "InvoiceArguments", "{\"methods\":[\"Cash\"]}"));
        assertThrows(IllegalArgumentException.class, () -> catalogue.validateAndCanonicalize(
            "InvoiceArguments", "{}"));
        assertThrows(IllegalArgumentException.class, () -> catalogue.validateAndCanonicalize(
            "InvoiceArguments", "{\"methods\":[]}"));
        assertThrows(IllegalArgumentException.class, () -> catalogue.validateAndCanonicalize(
            "InvoiceArguments", "{\"methods\":[\"Cash\",\"Accrual\",\"Cash\"]}"));
        assertThrows(IllegalArgumentException.class, () -> catalogue.validateAndCanonicalize(
            "InvoiceArguments", "{\"methods\":[\"Hybrid\"]}"));
    }
}
