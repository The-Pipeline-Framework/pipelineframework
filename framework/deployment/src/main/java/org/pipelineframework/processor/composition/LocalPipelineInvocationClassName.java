/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.pipelineframework.processor.composition;

import java.util.Objects;

/** Deterministic generated class naming shared by invocation source and metadata renderers. */
public final class LocalPipelineInvocationClassName {
    private LocalPipelineInvocationClassName() {
    }

    public static String simpleName(CompiledPipelineLocation location) {
        String normalized = Objects.requireNonNull(location, "location must not be null").display()
            .replaceAll("[^A-Za-z0-9_]", "_")
            .replaceAll("_+", "_")
            .replaceFirst("^_+", "");
        return "PipelineInvocation_" + normalized;
    }

    public static String canonicalName(String basePackage, CompiledPipelineLocation location) {
        String normalizedPackage = Objects.requireNonNull(basePackage, "basePackage must not be null").strip();
        if (normalizedPackage.isEmpty()) {
            throw new IllegalArgumentException("basePackage must not be blank");
        }
        return normalizedPackage + ".pipeline." + simpleName(location);
    }
}
