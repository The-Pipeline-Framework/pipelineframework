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

/**
 * Full pipeline template configuration loaded from the pipeline template YAML file.
 *
 * @param appName the application name
 * @param basePackage the base Java package
 * @param transport the global transport (GRPC or REST)
 * @param steps the pipeline steps defined in the template
 * @param aspects the aspect configurations keyed by aspect name
 */
public record PipelineTemplateConfig(
    String appName,
    String basePackage,
    String transport,
    List<PipelineTemplateStep> steps,
    Map<String, PipelineTemplateAspect> aspects
) {
}
