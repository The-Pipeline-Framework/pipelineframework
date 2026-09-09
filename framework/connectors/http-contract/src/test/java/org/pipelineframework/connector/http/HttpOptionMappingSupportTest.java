package org.pipelineframework.connector.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.junit.jupiter.api.Test;

class HttpOptionMappingSupportTest {
    @Test
    void mapsFieldsConstantsEnumsCollectionsAndDiscriminatorsDeterministically() {
        HttpRepresentationMappingOptions options = new HttpRepresentationMappingOptions(
            Map.of("subject", "request.subject"),
            Map.of("request.version", JsonNodeFactory.instance.numberNode(1)),
            Map.of("status", Map.of("OPEN", "open")),
            Map.of("metadataJson", "request.metadata"),
            List.of(new HttpRepresentationMappingOptions.CollectionMapping(
                "items", "request.lines", Map.of("value", "text"))),
            Optional.of(new HttpRepresentationMappingOptions.DiscriminatorMapping(
                "kind", "request.type", Map.of("LOOKUP", "lookup"))));

        var canonical = HttpPinnedJson.parse("""
            {"subject":"acme","status":"OPEN","kind":"LOOKUP","metadataJson":"{\\"flag\\":true}","items":[{"value":"one"}]}
            """);
        var wire = HttpOptionMappingSupport.toWire(canonical, options);

        assertEquals(HttpPinnedJson.parse("""
            {"request":{"subject":"acme","version":1,"metadata":{"flag":true},"lines":[{"text":"one"}],"type":"lookup"},"status":"open"}
            """), wire);
        assertEquals(canonical, HttpOptionMappingSupport.fromWire(wire, options));
    }

    @Test
    void rejectsUnknownOptionFieldsAndNonInvertibleEnums() {
        assertThrows(IllegalArgumentException.class,
            () -> HttpRepresentationMappingOptions.from(Map.of("script", "return input")));
        assertThrows(IllegalArgumentException.class, () -> new HttpRepresentationMappingOptions(
            Map.of(), Map.of(), Map.of("state", Map.of("A", "same", "B", "same")), Map.of(),
            List.of(), Optional.empty()));
        assertThrows(IllegalArgumentException.class, () -> new HttpRepresentationMappingOptions(
            Map.of("value", "wire", " value ", "other"), Map.of(), Map.of(), Map.of(),
            List.of(), Optional.empty()));
        assertThrows(IllegalArgumentException.class, () -> new HttpRepresentationMappingOptions(
            Map.of(), Map.of(), Map.of("state", Map.of("OPEN", "open", "CLOSED", " open ")), Map.of(),
            List.of(), Optional.empty()));
    }
}
