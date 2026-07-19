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

package org.pipelineframework.processor.phase;

import org.pipelineframework.config.template.*;

/**
 * Deployment-side capability boundary for the first Java v3 target slice.
 *
 * <p>This validator deliberately contains no mapper or renderer policy. It only prevents a v3
 * template from entering deployment phases that cannot preserve its domain representation.</p>
 */
final class V3JavaWorkloadCapabilityValidator {

    void validate(PipelineTemplateConfig config) {
        if (config.dialect() != PipelineTemplateDialect.V3) {
            return;
        }
        for (PipelineTemplateStep step : config.steps()) {
            if (step.execution() != null && step.execution().isRemote()) {
                throw new IllegalStateException("Version: 3 Java workload realization supports local inspectable steps only; "
                    + "remote step '" + step.name() + "' remains pending.");
            }
            if (!"ONE_TO_ONE".equalsIgnoreCase(step.cardinality())) {
                throw new IllegalStateException("Version: 3 Java workload realization supports ONE_TO_ONE steps only; step '"
                    + step.name() + "' declares '" + step.cardinality() + "'.");
            }
        }
    }
}
