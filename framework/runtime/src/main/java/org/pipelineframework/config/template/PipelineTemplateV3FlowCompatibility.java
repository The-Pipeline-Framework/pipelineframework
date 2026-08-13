/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

/**
 * Compiler-owned v3 compatibility for a value flowing into a later authored step.
 *
 * <p>This intentionally supplements, rather than changes, ordinary type assignability: a union
 * may supply one compatible payload to a later branch step. It is shared by the template loader
 * and composition linker so nested definitions cannot narrow the v3 language.
 */
public final class PipelineTemplateV3FlowCompatibility {

    private PipelineTemplateV3FlowCompatibility() {
    }

    public static boolean compatible(PipelineTemplateTypeModel typeModel, String source, String target) {
        if (typeModel.isAssignable(source, target)) {
            return true;
        }
        if (typeModel.definition(target)
            .filter(PipelineTemplateTypeDefinition.UnionType.class::isInstance)
            .isPresent()) {
            return false;
        }
        return typeModel.definition(source)
            .filter(PipelineTemplateTypeDefinition.UnionType.class::isInstance)
            .map(PipelineTemplateTypeDefinition.UnionType.class::cast)
            .map(union -> union.variants().values().stream()
                .map(PipelineTemplateTypeDefinition.Variant::payload)
                .anyMatch(payload -> typeModel.isAssignable(payload.name(), target)))
            .orElse(false);
    }
}
