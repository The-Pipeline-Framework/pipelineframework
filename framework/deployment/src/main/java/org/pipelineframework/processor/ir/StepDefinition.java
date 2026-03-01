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

package org.pipelineframework.processor.ir;

import java.util.Objects;
import javax.annotation.Nullable;
import com.squareup.javapoet.ClassName;

/**
 * Represents a step definition parsed from YAML configuration.
 * This model drives the generation of step clients based on YAML declarations.
 *
 * @param name The name of the step
 * @param kind The kind of step (INTERNAL or DELEGATED)
 * @param executionClass The class that provides the execution implementation
 * @param externalMapper The operator mapper class for mapping between domain and operator types (nullable)
 * @param mapperFallback mapper fallback strategy when no mapper matches (never null)
 * @param inputType The input type for the step
 * @param outputType The output type for the step
 * @param streamingShapeHint Optional streaming shape hint parsed from YAML cardinality
 */
public record StepDefinition(
        String name,
        StepKind kind,
        ClassName executionClass,
        @Nullable ClassName externalMapper,
        MapperFallbackMode mapperFallback,
        @Nullable ClassName inputType,
        @Nullable ClassName outputType,
        @Nullable StreamingShape streamingShapeHint
) {
    public StepDefinition {
        Objects.requireNonNull(name, "Name cannot be null");
        if (name.isBlank()) {
            throw new IllegalArgumentException("Name cannot be blank");
        }
        Objects.requireNonNull(kind, "Kind cannot be null");
        Objects.requireNonNull(executionClass, "Execution class cannot be null");
        mapperFallback = mapperFallback == null ? MapperFallbackMode.NONE : mapperFallback;
    }
}
