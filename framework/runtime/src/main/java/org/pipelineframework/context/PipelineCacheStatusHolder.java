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
import org.pipelineframework.cache.CacheStatus;

/**
 * Holds cache status reported by cache plugin steps for the current request.
 */
public final class PipelineCacheStatusHolder {

    private static final String CONTEXT_KEY = PipelineCacheStatusHolder.class.getName() + ".status";
    private static final ThreadLocal<CacheStatus> THREAD_LOCAL = new ThreadLocal<>();

    private PipelineCacheStatusHolder() {
    }

    /**
     * Returns the cache status for the current request context.
     *
     * @return the cache status, or null if none is set
     */
    public static CacheStatus get() {
        Context context = Vertx.currentContext();
        if (context != null) {
            try {
                Object stored = context.getLocal(CONTEXT_KEY);
                if (stored instanceof CacheStatus status) {
                    return status;
                }
            } catch (UnsupportedOperationException ignored) {
                // Root contexts forbid locals; fall back to thread-local storage.
            }
        }
        return THREAD_LOCAL.get();
    }

    /**
     * Returns the cache status and clears it from the current context.
     *
     * @return the cache status, or null if none is set
     */
    public static CacheStatus getAndClear() {
        CacheStatus status = get();
        clear();
        return status;
    }

    /**
     * Sets the cache status for the current request context.
     *
     * @param status the cache status to store
     */
    public static void set(CacheStatus status) {
        Context context = Vertx.currentContext();
        if (context != null) {
            try {
                if (status == null) {
                    context.removeLocal(CONTEXT_KEY);
                } else {
                    context.putLocal(CONTEXT_KEY, status);
                }
                return;
            } catch (UnsupportedOperationException ignored) {
                // Root contexts forbid locals; fall back to thread-local storage.
            }
        }
        if (status == null) {
            THREAD_LOCAL.remove();
        } else {
            THREAD_LOCAL.set(status);
        }
    }

    /**
     * Clears the cache status from the current request context.
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
