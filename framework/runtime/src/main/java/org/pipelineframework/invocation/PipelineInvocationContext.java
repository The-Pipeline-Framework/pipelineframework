/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.pipelineframework.invocation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Immutable runtime-local identity and bound for structured recursive invocation. */
public record PipelineInvocationContext(List<Frame> recursivePath, int maxRecursiveDepth) {

    public PipelineInvocationContext {
        recursivePath = List.copyOf(Objects.requireNonNull(recursivePath, "recursivePath must not be null"));
        recursivePath.forEach(frame -> Objects.requireNonNull(frame, "recursivePath must not contain null"));
        if (maxRecursiveDepth < 0) {
            throw new IllegalArgumentException("maxRecursiveDepth must be non-negative");
        }
    }

    public static PipelineInvocationContext root(int maxRecursiveDepth) {
        return new PipelineInvocationContext(List.of(), maxRecursiveDepth);
    }

    public PipelineInvocationContext enterRecursive(String definitionId, String callsiteId) {
        String definition = required(definitionId, "definitionId");
        String callsite = required(callsiteId, "callsiteId");
        int nextDepth = recursivePath.size() + 1;
        if (nextDepth > maxRecursiveDepth) {
            throw new PipelineRecursionLimitExceededException(
                definition, callsite, nextDepth, maxRecursiveDepth, recursivePath);
        }
        long occurrence = recursivePath.stream()
            .filter(frame -> frame.definitionId().equals(definition) && frame.callsiteId().equals(callsite))
            .count() + 1;
        List<Frame> childPath = new ArrayList<>(recursivePath);
        childPath.add(new Frame(definition, callsite, occurrence));
        return new PipelineInvocationContext(childPath, maxRecursiveDepth);
    }

    public int recursiveDepth() {
        return recursivePath.size();
    }

    public record Frame(String definitionId, String callsiteId, long occurrence) {
        public Frame {
            definitionId = required(definitionId, "definitionId");
            callsiteId = required(callsiteId, "callsiteId");
            if (occurrence < 1) {
                throw new IllegalArgumentException("occurrence must be positive");
            }
        }

        public String display() {
            return definitionId + ":" + callsiteId + "#" + occurrence;
        }
    }

    private static String required(String value, String field) {
        String normalized = Objects.requireNonNull(value, field + " must not be null").strip();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return normalized;
    }
}
