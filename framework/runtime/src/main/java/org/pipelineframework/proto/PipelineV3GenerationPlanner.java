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

import org.pipelineframework.config.template.PipelineIdlSnapshot;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateDialect;

/** Builds the common v3 plan after the compiler has resolved wire state. */
final class PipelineV3GenerationPlanner {

    PipelineV3GenerationPlan plan(PipelineTemplateConfig config, PipelineIdlSnapshot idlState) {
        if (config.dialect() != PipelineTemplateDialect.V3) {
            throw new IllegalArgumentException("A v3 generation plan requires a version: 3 template");
        }
        return new PipelineV3GenerationPlan(config.basePackage(), config.typeModel(), idlState);
    }
}
