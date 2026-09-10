/*
 * Copyright (c) 2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.pipelineframework.processor.awaitable;

import java.util.Objects;
import java.util.Optional;

import com.squareup.javapoet.TypeName;

/** Compiler-resolved Java boundary for an authored operation decorated with deferred completion. */
public record AwaitStepTypeBinding(
    TypeName operationOutputType,
    TypeName finalOutputType,
    String finalOutputCanonicalType,
    Optional<String> completionPayloadCanonicalType,
    Optional<TypeName> completionPayloadType
) {
    public AwaitStepTypeBinding {
        Objects.requireNonNull(operationOutputType, "operationOutputType");
        Objects.requireNonNull(finalOutputType, "finalOutputType");
        Objects.requireNonNull(finalOutputCanonicalType, "finalOutputCanonicalType");
        completionPayloadCanonicalType = completionPayloadCanonicalType == null
            ? Optional.empty() : completionPayloadCanonicalType;
        completionPayloadType = completionPayloadType == null ? Optional.empty() : completionPayloadType;
    }
}
