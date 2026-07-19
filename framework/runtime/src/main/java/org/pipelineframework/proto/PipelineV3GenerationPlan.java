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

package org.pipelineframework.proto;

import java.util.Objects;

import org.pipelineframework.config.template.PipelineIdlSnapshot;
import org.pipelineframework.config.template.PipelineTemplateTypeModel;

/** Immutable v3 target-generation input shared by every target renderer. */
record PipelineV3GenerationPlan(
    String basePackage,
    PipelineTemplateTypeModel typeModel,
    PipelineIdlSnapshot idlState
) {
    PipelineV3GenerationPlan {
        if (basePackage == null || basePackage.isBlank()) {
            throw new IllegalArgumentException("Version 3 generation requires a base package");
        }
        typeModel = Objects.requireNonNull(typeModel, "typeModel");
        idlState = Objects.requireNonNull(idlState, "idlState");
    }
}
