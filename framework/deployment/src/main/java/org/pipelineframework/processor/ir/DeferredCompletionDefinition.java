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

package org.pipelineframework.processor.ir;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.squareup.javapoet.ClassName;

/**
 * Typed compiler input for durable deferred completion attached to an authored operation.
 *
 * <p>The authored operation produces {@code operationOutput}; the pipeline-visible output is
 * supplied later by the configured completion transport.</p>
 */
public record DeferredCompletionDefinition(
    String operationOutputType,
    Optional<ClassName> operationOutputJavaType,
    String timeout,
    List<String> idempotencyKeyFields,
    String correlationStrategy,
    String transportType,
    Map<String, Object> transportConfig,
    Optional<CompletionProjectionDefinition> completion
) {
    public DeferredCompletionDefinition {
        operationOutputType = requireText(operationOutputType, "operationOutput.type");
        operationOutputJavaType = operationOutputJavaType == null ? Optional.empty() : operationOutputJavaType;
        timeout = requireText(timeout, "timeout");
        idempotencyKeyFields = idempotencyKeyFields == null ? List.of() : List.copyOf(idempotencyKeyFields);
        correlationStrategy = requireText(correlationStrategy, "correlation.strategy");
        transportType = requireText(transportType, "transport.type");
        transportConfig = transportConfig == null ? Map.of() : Map.copyOf(transportConfig);
        completion = completion == null ? Optional.empty() : completion;
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value.trim();
    }

    /** Request-aware projection from an untrusted completion payload to the step output. */
    public record CompletionProjectionDefinition(String type, ClassName projector) {
        public CompletionProjectionDefinition {
            type = requireText(type, "completion.type");
            Objects.requireNonNull(projector, "completion.projector must not be null");
        }
    }
}
