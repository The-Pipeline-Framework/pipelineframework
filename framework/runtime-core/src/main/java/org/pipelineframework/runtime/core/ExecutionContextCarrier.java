/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

package org.pipelineframework.runtime.core;

import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * Neutral abstraction for runtime-local execution metadata propagation.
 */
public interface ExecutionContextCarrier {

    /**
     * Captures the caller's context and returns a task that installs it only while it executes.
     */
    default <T> Callable<T> contextualize(Callable<T> task) {
        return Objects.requireNonNull(task, "contextualized task must not be null");
    }

    /**
     * Reads a typed context value by key.
     *
     * @param key context key
     * @param type expected value type
     * @param <T> expected value type
     * @return typed value if present and compatible
     */
    <T> T get(String key, Class<T> type);

    /**
     * Writes a context value for the given key.
     *
     * @param key context key
     * @param value value to publish
     */
    void put(String key, Object value);

    /**
     * Clears the context value for the given key.
     *
     * @param key context key
     */
    void clear(String key);

    /**
     * Clears all context values managed by this carrier.
     */
    void clear();
}
