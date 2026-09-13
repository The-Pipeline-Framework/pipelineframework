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

/**
 * Request-aware Await completion configuration.
 *
 * @param type actor-supplied completion payload type
 * @param projector pure {@code AwaitCompletionProjector} implementation
 */
public record PipelineYamlAwaitCompletion(String type, String projector) {
    public PipelineYamlAwaitCompletion {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("await.completion.type must be defined");
        }
        if (projector == null || projector.isBlank()) {
            throw new IllegalArgumentException("await.completion.projector must be defined");
        }
        type = type.trim();
        projector = projector.trim();
    }
}
