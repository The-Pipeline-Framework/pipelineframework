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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class LegacyPipelineTemplateTypeModelAdapterTest {

    private final LegacyPipelineTemplateTypeModelAdapter adapter = new LegacyPipelineTemplateTypeModelAdapter();

    @Test
    void convertsLegacyMessagesAndUnionsToCanonicalDefinitions() {
        Map<String, PipelineTemplateMessage> messages = messages();
        Map<String, PipelineTemplateUnion> unions = unions();

        PipelineTemplateTypeModel model = adapter.adapt(messages, unions);

        assertEquals(expectedDefinitions(), model.definitions());
        assertTrue(adapter.adapt(null, null).definitions().isEmpty());
    }

    @Test
    void configConstructionPathsRetainLegacyConversionBehavior() {
        Map<String, PipelineTemplateMessage> messages = messages();
        Map<String, PipelineTemplateUnion> unions = unions();

        PipelineTemplateConfig canonical = new PipelineTemplateConfig(
            2, "legacy", "com.example", "LOCAL", PipelinePlatform.COMPUTE,
            messages, unions, Map.of(), Map.of(), List.of(), Map.of(), null, null, null,
            null, null, null, Map.of());
        PipelineTemplateConfig compatibility = new PipelineTemplateConfig(
            2, "legacy", "com.example", "LOCAL", PipelinePlatform.COMPUTE,
            messages, unions, Map.of(), Map.of(), List.of(), Map.of(), null, null, null,
            null, null);

        assertEquals(expectedDefinitions(), canonical.typeModel().definitions());
        assertEquals(expectedDefinitions(), compatibility.typeModel().definitions());
    }

    private static Map<String, PipelineTemplateMessage> messages() {
        PipelineTemplateField id = new PipelineTemplateField(
            1, "id", "string", "string", null, "String", "string", null, null,
            false, false, false, null, null, null, null, null);
        PipelineTemplateField customer = new PipelineTemplateField(
            1, "customer", "Customer", "message", "Customer", "Customer", "Customer", null, null,
            false, false, false, null, null, null, null, null);
        PipelineTemplateField customersById = new PipelineTemplateField(
            2, "customersById", "map", "map", null, "Map<String, Customer>", "Customer", "string", "Customer",
            false, false, false, null, null, null, null, null);
        return Map.of(
            "Customer", new PipelineTemplateMessage("Customer", List.of(id), null),
            "Envelope", new PipelineTemplateMessage("Envelope", List.of(customer, customersById), null));
    }

    private static Map<String, PipelineTemplateUnion> unions() {
        return Map.of("Choice", new PipelineTemplateUnion("Choice", Map.of(
            "customer", new PipelineTemplateUnionVariant("customer", "Customer", 1))));
    }

    private static Map<String, PipelineTemplateTypeDefinition> expectedDefinitions() {
        return Map.of(
            "Customer", new PipelineTemplateTypeDefinition.RecordType("Customer", List.of(
                new PipelineTemplateTypeDefinition.Field(
                    "id", new PipelineTemplateTypeReference.Scalar("string"), false))),
            "Envelope", new PipelineTemplateTypeDefinition.RecordType("Envelope", List.of(
                new PipelineTemplateTypeDefinition.Field(
                    "customer", new PipelineTemplateTypeReference.Named("Customer"), false),
                new PipelineTemplateTypeDefinition.Field(
                    "customersById", new PipelineTemplateTypeReference.MapType(
                        new PipelineTemplateTypeReference.Scalar("string"),
                        new PipelineTemplateTypeReference.Named("Customer")), false))),
            "Choice", new PipelineTemplateTypeDefinition.UnionType("Choice", Map.of(
                "customer", new PipelineTemplateTypeDefinition.Variant(
                    "customer", new PipelineTemplateTypeReference.Named("Customer"))))
        );
    }
}
