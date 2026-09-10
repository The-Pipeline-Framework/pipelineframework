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

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;

/** Compiler-resolved deferred-completion contract decorating an ordinary authored operation. */
public record DeferredCompletionSelection(
    TypeName finalOutputType,
    String finalOutputCanonicalType,
    Optional<String> completionPayloadCanonicalType,
    Duration timeout,
    List<String> idempotencyKeyFields,
    String correlationStrategy,
    String transportType,
    Map<String, Object> transportConfig,
    Optional<TypeName> completionPayloadType,
    Optional<ClassName> completionProjector
) {
    public DeferredCompletionSelection {
        Objects.requireNonNull(finalOutputType, "finalOutputType");
        finalOutputCanonicalType = requireText(finalOutputCanonicalType, "finalOutputCanonicalType");
        completionPayloadCanonicalType = completionPayloadCanonicalType == null
            ? Optional.empty() : completionPayloadCanonicalType.map(String::trim).filter(value -> !value.isEmpty());
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        idempotencyKeyFields = idempotencyKeyFields == null ? List.of() : List.copyOf(idempotencyKeyFields);
        correlationStrategy = requireText(correlationStrategy, "correlationStrategy");
        transportType = requireText(transportType, "transportType");
        transportConfig = transportConfig == null ? Map.of() : Map.copyOf(transportConfig);
        completionPayloadType = completionPayloadType == null ? Optional.empty() : completionPayloadType;
        completionProjector = completionProjector == null ? Optional.empty() : completionProjector;
        if (completionPayloadType.isPresent() != completionProjector.isPresent()) {
            throw new IllegalArgumentException("completion payload type and projector must be declared together");
        }
    }

    private static String requireText(String value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value.trim();
    }
}
