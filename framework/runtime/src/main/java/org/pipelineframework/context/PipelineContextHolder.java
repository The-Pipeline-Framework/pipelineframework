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

package org.pipelineframework.context;

import io.vertx.core.Context;
import io.vertx.core.Vertx;

/**
 * Holds the current PipelineContext using Vert.x context when available, and
 * falls back to a ThreadLocal otherwise.
 */
public final class PipelineContextHolder {

    private static final String CONTEXT_KEY = PipelineContextHolder.class.getName() + ".context";
    private static final ThreadLocal<PipelineContext> THREAD_LOCAL = new ThreadLocal<>();

    private PipelineContextHolder() {
    }

    /**
     * Returns the current pipeline context.
     *
     * @return the pipeline context, or null if none is set
     */
    public static PipelineContext get() {
        Context context = Vertx.currentContext();
        if (context != null) {
            try {
                Object stored = context.getLocal(CONTEXT_KEY);
                if (stored instanceof PipelineContext pipelineContext) {
                    return pipelineContext;
                }
            } catch (UnsupportedOperationException ignored) {
                // Root contexts forbid locals; fall back to thread-local storage.
            }
        }
        return THREAD_LOCAL.get();
    }

    /**
     * Sets the current pipeline context.
     *
     * @param pipelineContext the context to store
     */
    public static void set(PipelineContext pipelineContext) {
        Context context = Vertx.currentContext();
        if (context != null) {
            try {
                context.putLocal(CONTEXT_KEY, pipelineContext);
                return;
            } catch (UnsupportedOperationException ignored) {
                // Root contexts forbid locals; fall back to thread-local storage.
            }
        }
        THREAD_LOCAL.set(pipelineContext);
    }

    /**
     * Clears the current pipeline context.
     */
    public static void clear() {
        Context context = Vertx.currentContext();
        if (context != null) {
            try {
                context.removeLocal(CONTEXT_KEY);
            } catch (UnsupportedOperationException ignored) {
                // Root contexts forbid locals; fall back to thread-local storage.
            }
        }
        THREAD_LOCAL.remove();
    }
}
