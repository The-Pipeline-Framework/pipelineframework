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

import java.util.Map;

/**
 * Aspect configuration from the pipeline template definition.
 *
 * @param enabled whether the aspect is enabled
 * @param scope the aspect scope (GLOBAL or STEPS)
 * @param position the aspect position (BEFORE_STEP or AFTER_STEP)
 * @param order the aspect order
 * @param config the free-form aspect configuration map
 */
public record PipelineTemplateAspect(
    boolean enabled,
    String scope,
    String position,
    int order,
    Map<String, Object> config
) {
}
