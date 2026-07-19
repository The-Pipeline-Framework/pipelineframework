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

import java.nio.file.Path;

import org.pipelineframework.config.template.PipelineIdlSnapshot;
import org.pipelineframework.config.template.PipelineTemplateConfig;

/** Coordinates v3 target renderers without assigning target logic to the proto facade. */
final class PipelineV3GenerationCoordinator {
    private final PipelineV3GenerationPlanner planner = new PipelineV3GenerationPlanner();
    private final PipelineJavaDomainGenerator javaGenerator = new PipelineJavaDomainGenerator();

    PipelineV3GenerationPlan plan(PipelineTemplateConfig config, PipelineIdlSnapshot state) {
        return planner.plan(config, state);
    }

    void generateJava(Path outputDirectory, PipelineV3GenerationPlan plan) {
        javaGenerator.generate(outputDirectory, plan);
    }
}
