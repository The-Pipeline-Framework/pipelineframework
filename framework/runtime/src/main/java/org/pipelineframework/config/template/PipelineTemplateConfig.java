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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Full pipeline template configuration loaded from the pipeline template YAML file.
 *
 * @param version template config schema version
 * @param appName application name
 * @param basePackage base Java package
 * @param transport global transport
 * @param platform runtime/deployment platform mode
 * @param messages top-level named messages
 * @param steps pipeline steps
 * @param aspects aspect configurations keyed by aspect name
 */
public record PipelineTemplateConfig(
    int version,
    String appName,
    String basePackage,
    String transport,
    PipelinePlatform platform,
    Map<String, PipelineTemplateMessage> messages,
    List<PipelineTemplateStep> steps,
    Map<String, PipelineTemplateAspect> aspects
) {
    public PipelineTemplateConfig {
        messages = messages == null ? Map.of() : Map.copyOf(messages);
        // Preserve null step placeholders so downstream phases/tests can explicitly skip them.
        steps = steps == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(steps));
        aspects = aspects == null ? Map.of() : Map.copyOf(aspects);
    }

    /**
     * Backward-compatible constructor used by existing code paths.
     *
     * @param appName app name
     * @param basePackage base package
     * @param transport transport
     * @param steps steps
     * @param aspects aspects
     */
    public PipelineTemplateConfig(
        String appName,
        String basePackage,
        String transport,
        List<PipelineTemplateStep> steps,
        Map<String, PipelineTemplateAspect> aspects
    ) {
        this(1, appName, basePackage, transport, PipelinePlatform.COMPUTE, Map.of(), steps, aspects);
    }

    public PipelineTemplateConfig(
        String appName,
        String basePackage,
        String transport,
        PipelinePlatform platform,
        List<PipelineTemplateStep> steps,
        Map<String, PipelineTemplateAspect> aspects
    ) {
        this(1, appName, basePackage, transport, platform, Map.of(), steps, aspects);
    }
}
