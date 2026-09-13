/*
 * Copyright (c) 2026 Mariano Barcia
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.protocol.ProtocolTypeIdentity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SemanticTypeVocabularyTest {

    @Test
    void normalizesScalarAndNamedTypeReferences() {
        assertTrue(PipelineTemplateScalarTypes.isScalar(" UUID "));
        assertFalse(PipelineTemplateScalarTypes.isScalar("custom"));
        assertEquals("uuid", new PipelineTemplateTypeReference.Scalar(" UUID ").name());
        assertEquals("Invoice", new PipelineTemplateTypeReference.Named(" Invoice ").name());
        assertEquals("remote.Payload", new PipelineTemplateTypeReference.Contributed(" remote.Payload ").name());
        assertEquals("map", new PipelineTemplateTypeReference.MapType(
            new PipelineTemplateTypeReference.Scalar("string"), new PipelineTemplateTypeReference.Named("Payload")
        ).name());
        assertThrows(IllegalArgumentException.class, () -> new PipelineTemplateTypeReference.Scalar("unknown"));
    }

    @Test
    void normalizesProtocolIdentityAndEnforcesItsProviderNamespace() {
        ConnectorProviderId provider = ConnectorProviderId.of("tpf.connector");
        ProtocolTypeIdentity identity = ProtocolTypeIdentity.of("tpf.connector.Payload");

        assertEquals(provider, identity.namespace());
        assertEquals("Payload", identity.typeName());
        assertEquals("tpf.connector.Payload", identity.qualifiedName());
        assertEquals(identity.qualifiedName(), identity.toString());
        assertTrue(provider.isFrameworkReserved());
        assertFalse(ConnectorProviderId.of("acme.connector").isFrameworkReserved());
        assertTrue(identity.compareTo(ProtocolTypeIdentity.of("tpf.connector.Result")) < 0);
        assertThrows(IllegalArgumentException.class, () -> ProtocolTypeIdentity.of("missing-type"));
    }

    @Test
    void keepsTypeDefinitionCollectionsImmutableAndFieldDefaultsCanonical() {
        List<PipelineTemplateTypeDefinition.Field> fields = new ArrayList<>();
        PipelineTemplateTypeDefinition.Field field = new PipelineTemplateTypeDefinition.Field(
            "id", new PipelineTemplateTypeReference.Scalar("uuid")
        );
        fields.add(field);
        PipelineTemplateTypeDefinition.RecordType record = new PipelineTemplateTypeDefinition.RecordType("Invoice", fields);
        fields.clear();

        Map<String, PipelineTemplateTypeDefinition.Variant> variants = new LinkedHashMap<>();
        variants.put("created", new PipelineTemplateTypeDefinition.Variant(
            "created", new PipelineTemplateTypeReference.Named("InvoiceCreated")
        ));
        PipelineTemplateTypeDefinition.UnionType union = new PipelineTemplateTypeDefinition.UnionType("Event", variants);
        variants.clear();

        assertEquals(List.of(field), record.fields());
        assertEquals(PipelineFieldPresence.REQUIRED, field.presence());
        assertEquals(PipelineFieldNullability.NON_NULL, field.nullability());
        assertTrue(field.constraints().isEmpty());
        assertEquals(1, union.variants().size());
        assertThrows(UnsupportedOperationException.class, () -> union.variants().clear());
        assertTrue(new PipelineTemplateTypeDefinition.WrapperType(
            "Quantity", new PipelineTemplateTypeReference.Scalar("int32")
        ).constraints().isEmpty());
    }
}
