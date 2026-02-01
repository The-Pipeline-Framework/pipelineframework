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

package org.pipelineframework.cache;

import java.util.Optional;

import io.smallrye.mutiny.Uni;

/**
 * SPI for reading cached pipeline outputs without coupling runtime to cache implementations.
 */
public interface PipelineCacheReader {

    /**
     * Retrieve a cached entry by key if supported.
     *
     * @param key cache key
     * @return the cached item if present
     */
    Uni<Optional<Object>> get(String key);

    /**
     * Determine if the cache contains the given key.
     *
     * @param key cache key
     * @return true if the key exists, false otherwise
     */
    Uni<Boolean> exists(String key);
}
