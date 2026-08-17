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

package org.pipelineframework.telemetry;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import org.pipelineframework.runtime.core.RuntimeAdapters;

/** Carries the owning root run context across reactive invocation boundaries. */
public final class PipelineRunContextHolder {

    private static final String CONTEXT_KEY = PipelineRunContextHolder.class.getName() + ".context";

    private PipelineRunContextHolder() {
    }

    public static Optional<PipelineRunContext> get() {
        return Optional.ofNullable(RuntimeAdapters.executionContext(CONTEXT_KEY, PipelineRunContext.class));
    }

    public static void set(PipelineRunContext context) {
        RuntimeAdapters.setExecutionContext(CONTEXT_KEY, Objects.requireNonNull(context, "context must not be null"));
    }

    public static void clear() {
        RuntimeAdapters.clearExecutionContext(CONTEXT_KEY);
    }

    public static <T> T call(PipelineRunContext context, Supplier<T> supplier) {
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(supplier, "supplier must not be null");
        Optional<PipelineRunContext> previous = get();
        set(context);
        try {
            return supplier.get();
        } finally {
            previous.ifPresentOrElse(PipelineRunContextHolder::set, PipelineRunContextHolder::clear);
        }
    }
}
