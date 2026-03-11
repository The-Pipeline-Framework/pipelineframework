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
import org.pipelineframework.config.template.PipelineTemplateStepExecution;

/**
 * Represents a step definition parsed from YAML configuration.
 * This model drives the generation of step clients based on YAML declarations.
 *
 * @param name The name of the step
 * @param kind The kind of step (INTERNAL, DELEGATED, or REMOTE)
 * @param executionClass The class that provides the execution implementation
 * @param remoteExecution remote execution metadata for REMOTE steps
 * @param externalMapper The operator mapper class for mapping between domain and operator types (nullable)
 * @param mapperFallback mapper fallback strategy when no mapper matches (never null)
 * @param inputType The input type for the step
 * @param outputType The output type for the step
 * @param streamingShapeHint Optional streaming shape hint parsed from YAML cardinality
 */
public record StepDefinition(
        String name,
        StepKind kind,
        @Nullable ClassName executionClass,
        @Nullable PipelineTemplateStepExecution remoteExecution,
        @Nullable ClassName externalMapper,
        MapperFallbackMode mapperFallback,
        @Nullable ClassName inputType,
        @Nullable ClassName outputType,
        @Nullable StreamingShape streamingShapeHint
) {
    public StepDefinition(
        String name,
        StepKind kind,
        @Nullable ClassName executionClass,
        @Nullable ClassName externalMapper,
        MapperFallbackMode mapperFallback,
        @Nullable ClassName inputType,
        @Nullable ClassName outputType,
        @Nullable StreamingShape streamingShapeHint
    ) {
        this(name, requireNonRemoteKind(kind), executionClass, null, externalMapper, mapperFallback, inputType, outputType,
            streamingShapeHint);
    }

    public StepDefinition {
        Objects.requireNonNull(name, "Name cannot be null");
        if (name.isBlank()) {
            throw new IllegalArgumentException("Name cannot be blank");
        }
        Objects.requireNonNull(kind, "Kind cannot be null");
        if (executionClass != null && remoteExecution != null) {
            throw new IllegalArgumentException("executionClass and remoteExecution are mutually exclusive");
        }
        if (kind == StepKind.REMOTE) {
            Objects.requireNonNull(remoteExecution, "Remote execution cannot be null for REMOTE steps");
            if (executionClass != null) {
                throw new IllegalArgumentException("REMOTE steps must not define an execution class");
            }
        } else {
            Objects.requireNonNull(executionClass, "Execution class cannot be null");
            if (remoteExecution != null) {
                throw new IllegalArgumentException("Only REMOTE steps may define remoteExecution");
            }
        }
        mapperFallback = mapperFallback == null ? MapperFallbackMode.NONE : mapperFallback;
    }

    private static StepKind requireNonRemoteKind(StepKind kind) {
        if (kind == StepKind.REMOTE) {
            throw new IllegalArgumentException("Convenience constructor cannot be used for StepKind.REMOTE; provide remoteExecution");
        }
        return kind;
    }
}
