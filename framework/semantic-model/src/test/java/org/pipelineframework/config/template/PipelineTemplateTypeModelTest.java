/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.config.template;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.protocol.ProtocolTypeIdentity;

class PipelineTemplateTypeModelTest {

    @Test
    void normalizesAndDefensivelyCopiesSemanticMetadata() {
        Map<String, PipelineTemplateTypeDefinition> definitions = new LinkedHashMap<>();
        definitions.put("Accepted", new PipelineTemplateTypeDefinition.RecordType("Accepted", List.of()));
        Map<String, Object> providerOptions = new LinkedHashMap<>();
        providerOptions.put("mode", "strict");
        RepresentationMapping mapping = new RepresentationMapping(
            "json", "Accepted", Optional.of(" com.example.AcceptedJson "), Optional.empty());

        PipelineTemplateTypeModel model = new PipelineTemplateTypeModel(
            definitions,
            Map.of("Accepted", Map.of("json", mapping)),
            Map.of("json", providerOptions),
            Map.of("Accepted", ProtocolTypeIdentity.of("example.Accepted")),
            Map.of("Accepted", "com.example.Accepted")
        );
        definitions.clear();
        providerOptions.put("mode", "relaxed");

        assertTrue(model.contains("Accepted"));
        assertEquals("strict", model.representationProviderConfiguration("json").orElseThrow().get("mode"));
        assertEquals(Optional.of("com.example.AcceptedJson"),
            model.representationMapping("Accepted", "json").orElseThrow().representationType());
        assertEquals("example.Accepted", model.contributedTypeIdentity("Accepted").orElseThrow().qualifiedName());
        assertEquals(Optional.of("com.example.Accepted"), model.javaTypeBinding("Accepted"));
        assertThrows(UnsupportedOperationException.class, () -> model.definitions().clear());
        assertThrows(UnsupportedOperationException.class,
            () -> model.representationProviderConfiguration("json").orElseThrow().clear());
    }

    @Test
    void resolvesAliasesAndPreservesNominalAndUnionAssignability() {
        PipelineTemplateTypeReference.Named accepted = new PipelineTemplateTypeReference.Named("Accepted");
        PipelineTemplateTypeModel model = new PipelineTemplateTypeModel(Map.of(
            "OrderId", new PipelineTemplateTypeDefinition.WrapperType(
                "OrderId", new PipelineTemplateTypeReference.Scalar("string")),
            "LegacyOrderId", new PipelineTemplateTypeDefinition.AliasType(
                "LegacyOrderId", new PipelineTemplateTypeReference.Named("OrderId")),
            "Accepted", new PipelineTemplateTypeDefinition.RecordType("Accepted", List.of()),
            "Rejected", new PipelineTemplateTypeDefinition.RecordType("Rejected", List.of()),
            "Outcome", new PipelineTemplateTypeDefinition.UnionType("Outcome", Map.of(
                "accepted", new PipelineTemplateTypeDefinition.Variant("accepted", accepted),
                "rejected", new PipelineTemplateTypeDefinition.Variant(
                    "rejected", new PipelineTemplateTypeReference.Named("Rejected"))))
        ));

        assertEquals(new PipelineTemplateTypeReference.Named("OrderId"),
            model.resolveAliases(new PipelineTemplateTypeReference.Named("LegacyOrderId")));
        assertTrue(model.isAssignable("LegacyOrderId", "OrderId"));
        assertTrue(model.isAssignable("Accepted", "Outcome"));
        assertFalse(model.isAssignable("string", "OrderId"));
        assertFalse(model.isAssignable("Outcome", "Accepted"));
    }

    @Test
    void rejectsInvalidReferencesAndBuiltInTypeNames() {
        IllegalStateException unknown = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateTypeModel(Map.of(
                "Invoice", new PipelineTemplateTypeDefinition.RecordType("Invoice", List.of(
                    new PipelineTemplateTypeDefinition.Field(
                        "customer", new PipelineTemplateTypeReference.Named("Customer")))))));
        IllegalStateException builtIn = assertThrows(IllegalStateException.class,
            () -> new PipelineTemplateTypeModel(Map.of(
                " string ", new PipelineTemplateTypeDefinition.RecordType(" string ", List.of()))));

        assertEquals("Type 'Invoice.customer' references unknown type 'Customer'", unknown.getMessage());
        assertEquals("Type name ' string ' conflicts with a built-in semantic type", builtIn.getMessage());
    }
}
