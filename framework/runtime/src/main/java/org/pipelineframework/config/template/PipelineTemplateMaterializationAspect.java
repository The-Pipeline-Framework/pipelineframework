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
import org.pipelineframework.materialization.MaterializationAction;
import org.pipelineframework.materialization.MaterializationPosition;
import org.pipelineframework.materialization.MaterializationScope;

/**
 * Aspect-like policy for transparent field materialization.
 */
public record PipelineTemplateMaterializationAspect(
    String name,
    boolean enabled,
    MaterializationScope scope,
    MaterializationPosition position,
    int order,
    MaterializationAction action,
    String message,
    List<String> fields,
    List<String> targetSteps
) {
    public PipelineTemplateMaterializationAspect {
        name = normalize(name);
        message = normalize(message);
        fields = normalizeList("fields", fields);
        targetSteps = normalizeList("targetSteps", targetSteps);
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    private static List<String> normalizeList(String name, List<String> values) {
        if (values == null) {
            return List.of();
        }
        return values.stream()
            .map(value -> {
                if (value == null || value.isBlank()) {
                    throw new IllegalArgumentException("materialization aspect " + name + " must not contain blank entries");
                }
                return value.trim();
            })
            .toList();
    }
}
