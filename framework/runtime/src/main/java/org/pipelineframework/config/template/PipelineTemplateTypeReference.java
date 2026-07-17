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

/** A resolved spelling-independent reference to either a scalar or named TPF type. */
public sealed interface PipelineTemplateTypeReference
    permits PipelineTemplateTypeReference.Scalar, PipelineTemplateTypeReference.Named,
    PipelineTemplateTypeReference.MapType {

    String name();

    record Scalar(String name) implements PipelineTemplateTypeReference {
        public Scalar {
            name = name == null ? null : name.trim().toLowerCase(java.util.Locale.ROOT);
            if (!PipelineTemplateTypeMappings.isV3ScalarType(name)) {
                throw new IllegalArgumentException("Unsupported scalar type '" + name + "'");
            }
        }
    }

    record Named(String name) implements PipelineTemplateTypeReference {
        public Named {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("Named type reference must not be blank");
            }
            name = name.trim();
        }
    }

    /** Legacy v2 map metadata retained by the normalized compatibility view. */
    record MapType(Scalar keyType, PipelineTemplateTypeReference valueType) implements PipelineTemplateTypeReference {
        public MapType {
            if (keyType == null || valueType == null) {
                throw new IllegalArgumentException("Map type references must declare key and value types");
            }
        }

        @Override
        public String name() {
            return "map";
        }
    }
}
