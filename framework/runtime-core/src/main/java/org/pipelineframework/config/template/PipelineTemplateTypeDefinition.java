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
import java.util.Objects;

/** Host-neutral normalized semantic v3 type declarations. Wire metadata is intentionally absent. */
public sealed interface PipelineTemplateTypeDefinition
    permits PipelineTemplateTypeDefinition.RecordType, PipelineTemplateTypeDefinition.WrapperType,
    PipelineTemplateTypeDefinition.AliasType, PipelineTemplateTypeDefinition.UnionType {

    String name();

    record RecordType(String name, List<Field> fields) implements PipelineTemplateTypeDefinition {
        public RecordType {
            fields = fields == null ? List.of() : List.copyOf(fields);
        }
    }

    record WrapperType(
        String name,
        PipelineTemplateTypeReference.Scalar wraps,
        PipelineTemplateWrapperConstraints constraints
    ) implements PipelineTemplateTypeDefinition {
        public WrapperType {
            constraints = constraints == null ? PipelineTemplateWrapperConstraints.empty() : constraints;
        }

        public WrapperType(String name, PipelineTemplateTypeReference.Scalar wraps) {
            this(name, wraps, PipelineTemplateWrapperConstraints.empty());
        }
    }

    record AliasType(String name, PipelineTemplateTypeReference target) implements PipelineTemplateTypeDefinition {
    }

    record UnionType(String name, Map<String, Variant> variants) implements PipelineTemplateTypeDefinition {
        public UnionType {
            variants = variants == null ? Map.of() : Map.copyOf(variants);
        }
    }

    record Field(
        String name,
        PipelineTemplateTypeReference type,
        boolean repeated,
        PipelineFieldPresence presence,
        PipelineFieldNullability nullability
    ) {
        public Field {
            Objects.requireNonNull(presence, "field presence must not be null");
            Objects.requireNonNull(nullability, "field nullability must not be null");
            if (repeated && (presence != PipelineFieldPresence.REQUIRED
                || nullability != PipelineFieldNullability.NON_NULL)) {
                throw new IllegalArgumentException("Repeated fields do not yet support presence or nullability modifiers.");
            }
        }

        public Field(String name, PipelineTemplateTypeReference type) {
            this(name, type, false, PipelineFieldPresence.REQUIRED, PipelineFieldNullability.NON_NULL);
        }

        public Field(String name, PipelineTemplateTypeReference type, boolean repeated) {
            this(name, type, repeated, PipelineFieldPresence.REQUIRED, PipelineFieldNullability.NON_NULL);
        }
    }

    record Variant(String discriminator, PipelineTemplateTypeReference payload) {
    }
}
