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

package org.pipelineframework.config.pipeline;

import java.util.List;

/**
 * Aspect configuration entry from the pipeline YAML file.
 *
 * @param name the aspect name
 * @param enabled whether the aspect is enabled
 * @param scope the aspect scope (GLOBAL or STEPS)
 * @param position the aspect position (BEFORE_STEP or AFTER_STEP)
 * @param targetSteps the list of target step names when scope is STEPS
 */
public record PipelineYamlAspect(
    String name,
    boolean enabled,
    String scope,
    String position,
    List<String> targetSteps
) {
}
