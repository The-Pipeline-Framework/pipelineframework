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
 * Pipeline configuration parsed from pipeline.yaml.
 *
 * @param basePackage the base package for generated pipeline classes
 * @param transport the transport mode (GRPC or REST)
 * @param steps the configured pipeline steps
 * @param aspects the configured pipeline aspects
 */
public record PipelineYamlConfig(
    String basePackage,
    String transport,
    List<PipelineYamlStep> steps,
    List<PipelineYamlAspect> aspects
) {
}
