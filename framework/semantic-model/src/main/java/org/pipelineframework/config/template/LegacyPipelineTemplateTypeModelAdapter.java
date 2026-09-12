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

import java.util.LinkedHashMap;
import java.util.Map;

/** Converts legacy authored type declarations into the shared semantic type model. */
final class LegacyPipelineTemplateTypeModelAdapter {

    PipelineTemplateTypeModel adapt(
        Map<String, PipelineTemplateMessage> messages,
        Map<String, PipelineTemplateUnion> unions
    ) {
        Map<String, PipelineTemplateTypeDefinition> definitions = new LinkedHashMap<>();
        if (messages != null) {
            messages.forEach((name, message) -> definitions.put(name,
                new PipelineTemplateTypeDefinition.RecordType(name, message.fields().stream()
                    .map(field -> new PipelineTemplateTypeDefinition.Field(
                        field.name(), legacyReference(field), field.repeated()))
                    .toList())));
        }
        if (unions != null) {
            unions.forEach((name, union) -> {
                Map<String, PipelineTemplateTypeDefinition.Variant> variants = new LinkedHashMap<>();
                union.variants().forEach((discriminator, variant) -> variants.put(discriminator,
                    new PipelineTemplateTypeDefinition.Variant(discriminator,
                        legacyReference(variant.type()))));
                definitions.put(name, new PipelineTemplateTypeDefinition.UnionType(name, variants));
            });
        }
        return new PipelineTemplateTypeModel(definitions);
    }

    private PipelineTemplateTypeReference legacyReference(PipelineTemplateField field) {
        if (field.isMap()) {
            return new PipelineTemplateTypeReference.MapType(
                new PipelineTemplateTypeReference.Scalar(field.keyType()), legacyReference(field.valueType()));
        }
        if (field.messageRef() != null && !field.messageRef().isBlank()) {
            return new PipelineTemplateTypeReference.Named(field.messageRef());
        }
        return new PipelineTemplateTypeReference.Scalar(field.canonicalType());
    }

    private PipelineTemplateTypeReference legacyReference(String type) {
        return PipelineTemplateTypeMappings.isV3ScalarType(type)
            ? new PipelineTemplateTypeReference.Scalar(type)
            : new PipelineTemplateTypeReference.Named(type);
    }
}
