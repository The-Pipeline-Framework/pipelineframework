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

import java.util.Optional;
import java.util.Objects;
import java.util.function.Supplier;
import org.pipelineframework.runtime.core.RuntimeAdapters;

/** Runtime adapter-backed carrier for the active structured invocation instance. */
public final class PipelineInvocationContextHolder {
    private static final String CONTEXT_KEY = PipelineInvocationContextHolder.class.getName() + ".context";

    private PipelineInvocationContextHolder() {
    }

    public static Optional<PipelineInvocationContext> get() {
        return Optional.ofNullable(RuntimeAdapters.executionContext(CONTEXT_KEY, PipelineInvocationContext.class));
    }

    public static void set(PipelineInvocationContext context) {
        RuntimeAdapters.setExecutionContext(CONTEXT_KEY, context);
    }

    public static void clear() {
        RuntimeAdapters.clearExecutionContext(CONTEXT_KEY);
    }

    public static <T> T call(PipelineInvocationContext context, Supplier<T> supplier) {
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(supplier, "supplier must not be null");
        Optional<PipelineInvocationContext> previous = get();
        set(context);
        try {
            return supplier.get();
        } finally {
            previous.ifPresentOrElse(PipelineInvocationContextHolder::set, PipelineInvocationContextHolder::clear);
        }
    }
}
