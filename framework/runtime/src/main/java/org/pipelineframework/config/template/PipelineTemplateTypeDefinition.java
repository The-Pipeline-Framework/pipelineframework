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

import java.util.List;
import java.util.Map;

/** Normalized semantic v3 type declarations. Wire metadata is intentionally absent. */
public sealed interface PipelineTemplateTypeDefinition
    permits PipelineTemplateTypeDefinition.RecordType, PipelineTemplateTypeDefinition.WrapperType,
    PipelineTemplateTypeDefinition.AliasType, PipelineTemplateTypeDefinition.UnionType {

    String name();

    record RecordType(String name, List<Field> fields) implements PipelineTemplateTypeDefinition {
        public RecordType {
            fields = fields == null ? List.of() : List.copyOf(fields);
        }
    }

    record WrapperType(String name, PipelineTemplateTypeReference.Scalar wraps) implements PipelineTemplateTypeDefinition {
    }

    record AliasType(String name, PipelineTemplateTypeReference target) implements PipelineTemplateTypeDefinition {
    }

    record UnionType(String name, Map<String, Variant> variants) implements PipelineTemplateTypeDefinition {
        public UnionType {
            variants = variants == null ? Map.of() : Map.copyOf(variants);
        }
    }

    record Field(String name, PipelineTemplateTypeReference type) {
    }

    record Variant(String discriminator, PipelineTemplateTypeReference payload) {
    }
}
