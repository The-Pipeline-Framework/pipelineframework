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

import org.pipelineframework.cache.CacheStatus;
import org.pipelineframework.runtime.core.RuntimeAdapters;

/**
 * Holds cache status reported by cache plugin steps for the current request.
 */
public final class PipelineCacheStatusHolder {

    private static final String CONTEXT_KEY = PipelineCacheStatusHolder.class.getName() + ".status";

    private PipelineCacheStatusHolder() {
    }

    /**
     * Returns the cache status for the current request context.
     *
     * @return the cache status, or null if none is set
     */
    public static CacheStatus get() {
        Object value = RuntimeAdapters.executionContext(CONTEXT_KEY, Object.class);
        if (value instanceof CacheStatus status) {
            return status;
        }
        return null;
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
        if (status == null) {
            RuntimeAdapters.clearExecutionContext(CONTEXT_KEY);
            return;
        }
        RuntimeAdapters.setExecutionContext(CONTEXT_KEY, status);
    }

    /**
     * Clears the cache status from the current request context.
     */
    public static void clear() {
        RuntimeAdapters.clearExecutionContext(CONTEXT_KEY);
    }
}
