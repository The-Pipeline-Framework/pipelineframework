package org.pipelineframework.connector.mcp.maven;

import org.pipelineframework.connector.mcp.McpInputSelection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;

class McpInputSelectionTest {
    @Test
    void preservesEquivalentNumericEnumAndConstOnSelectedFields() {
        var original = Map.<String, Object>of("type", "object", "additionalProperties", false,
            "properties", Map.of("value", Map.of("type", "number", "enum", List.of(1), "const", 1.0)));
        var projected = new McpInputSelection(List.of("value")).project(original);
        var types = new McpSchemaNormalizer().normalize("Request", projected, "numeric selection");
        var wrapper = assertInstanceOf(PipelineTemplateTypeDefinition.WrapperType.class,
            types.stream().filter(type -> type.identity().typeName().equals("RequestValueValue"))
                .findFirst().orElseThrow().definition());
        assertEquals(List.of(java.math.BigInteger.ONE), wrapper.constraints().allowedValues());
    }

    private final Map<String, Object> params = Map.of("type", "object", "additionalProperties", false,
        "required", List.of("customer_id", "line_items"), "properties", Map.of(
            "customer_id", Map.of("type", "string", "minLength", 1),
            "line_items", Map.of("type", "array", "minItems", 1, "maxItems", 3,
                "items", Map.of("type", "object", "additionalProperties", false, "required", List.of("amount"),
                    "properties", Map.of("amount", Map.of("type", "number", "minimum", 0)))),
            "global_tax_calculation", Map.of("type", "string", "enum", List.of("TaxExcluded", "TaxInclusive")),
            "due_date", Map.of("type", "string", "format", "date"),
            "linked_txn", Map.of("type", "array", "items", Map.of("oneOf", List.of()))));
    private final Map<String, Object> schema = Map.of("type", "object", "additionalProperties", false,
        "required", List.of("params"), "properties", Map.of("params", params));
    private final List<String> fields = List.of("params.customer_id", "params.line_items",
        "params.global_tax_calculation", "params.due_date");

    @Test
    void omitsOnlyUnselectedOptionalSubtreesAndRetainsCanonicalConstraints() {
        Map<String, Object> projected = new McpInputSelection(fields).project(schema);
        var types = new McpSchemaNormalizer().normalize("Invoice", projected, "create_invoice");
        PipelineTemplateTypeDefinition.RecordType record = assertInstanceOf(PipelineTemplateTypeDefinition.RecordType.class,
            types.stream().filter(type -> type.identity().typeName().equals("InvoiceParams")).findFirst().orElseThrow()
                .definition());
        assertEquals(List.of("customer_id", "due_date", "global_tax_calculation", "line_items"),
            record.fields().stream().map(PipelineTemplateTypeDefinition.Field::name).toList());
        var lines = record.fields().getLast();
        assertEquals(1, lines.constraints().minItems().orElseThrow());
        assertEquals(3, lines.constraints().maxItems().orElseThrow());
        var tax = assertInstanceOf(PipelineTemplateTypeDefinition.WrapperType.class,
            types.stream().filter(type -> type.identity().typeName().equals("InvoiceParamsGlobalTaxCalculationValue"))
                .findFirst().orElseThrow().definition());
        assertEquals(List.of("TaxExcluded", "TaxInclusive"), tax.constraints().allowedValues());
        assertTrue(schema.toString().contains("linked_txn"));
        assertFalse(projected.toString().contains("linked_txn"));
        assertEquals(params, ((Map<?, ?>) schema.get("properties")).get("params"));
    }

    @Test
    void emptySelectionOrSelectingWholeParentRetainsFailClosedProjection() {
        for (List<String> selection : List.of(List.<String>of(), List.of("params"))) {
            Map<String, Object> projected = new McpInputSelection(selection).project(schema);
            assertThrows(IllegalArgumentException.class,
                () -> new McpSchemaNormalizer().normalize("Invoice", projected, "create_invoice"));
        }
    }

    @Test
    void reportsExactRequiredUnknownAndIncompletePaths() {
        assertPath(List.of("params.customer_id"), "$.properties.params.properties.line_items");
        assertPath(List.of("params.unknown"), "$.properties.params.properties.unknown");
        assertPath(List.of("params.customer_id.nope", "params.line_items"), "$.properties.params.properties.customer_id");
        assertPath(List.of("params.customer_id", "params.line_items.amount"), "$.properties.params.properties.line_items");
    }

    @Test
    void rejectsDuplicateOverlappingOrMalformedPathsAndCanonicalizesOrder() {
        for (List<String> invalid : List.of(List.of("params", "params"), List.of("params", "params.customer_id"),
            List.of("params..id"), List.of("params."), List.of(" params.id"))) {
            assertThrows(IllegalArgumentException.class, () -> new McpInputSelection(invalid));
        }
        var reverse = new java.util.ArrayList<>(fields);
        java.util.Collections.reverse(reverse);
        assertEquals(new McpInputSelection(fields), new McpInputSelection(reverse));
        assertEquals(new McpInputSelection(fields).project(schema), new McpInputSelection(reverse).project(schema));
    }

    @Test
    void preservesParentConstraintsSoUnsupportedCrossFieldRulesStillFail() {
        for (String keyword : List.of("dependentRequired", "dependencies", "unknownConstraint")) {
            var parent = new java.util.LinkedHashMap<>(params);
            parent.put(keyword, Map.of("customer_id", List.of("linked_txn")));
            var original = new java.util.LinkedHashMap<>(schema);
            original.put("properties", Map.of("params", parent));
            var projected = new McpInputSelection(fields).project(original);
            assertTrue(assertThrows(IllegalArgumentException.class,
                () -> new McpSchemaNormalizer().normalize("Invoice", projected, "create_invoice"))
                .getMessage().contains("$.properties.params." + keyword));
        }
    }

    private void assertPath(List<String> selected, String path) {
        assertTrue(assertThrows(IllegalArgumentException.class,
            () -> new McpInputSelection(selected).project(schema)).getMessage().contains(path));
    }
}
